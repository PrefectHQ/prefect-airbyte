"""Tasks for connecting to Airbyte and triggering connection syncs"""
import uuid
from asyncio import sleep
from datetime import datetime
from typing import Any, Dict, Optional
from warnings import warn

from prefect import get_run_logger, task
from prefect.blocks.abstract import JobBlock, JobRun
from prefect.utilities.asyncutils import sync_compatible
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import BaseModel, Field
else:
    from pydantic import BaseModel, Field

from typing_extensions import Literal

from prefect_airbyte import exceptions as err
from prefect_airbyte.server import AirbyteServer

# Connection statuses
CONNECTION_STATUS_ACTIVE = "active"
CONNECTION_STATUS_INACTIVE = "inactive"
CONNECTION_STATUS_DEPRECATED = "deprecated"

# Job statuses
JOB_STATUS_CANCELLED = "cancelled"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_PENDING = "pending"
JOB_STATUS_SUCCEEDED = "succeeded"

terminal_job_statuses = {
    JOB_STATUS_CANCELLED,
    JOB_STATUS_FAILED,
    JOB_STATUS_SUCCEEDED,
}


@task
async def trigger_sync(
    connection_id: str,
    airbyte_server: Optional[AirbyteServer] = None,
    airbyte_server_host: Optional[str] = None,
    airbyte_server_port: Optional[int] = None,
    airbyte_api_version: Optional[str] = None,
    poll_interval_s: int = 15,
    status_updates: bool = False,
    timeout: int = 5,
) -> Dict[str, Any]:
    """Prefect Task for triggering an Airbyte connection sync.

    *It is assumed that the user will have previously configured
    a Source & Destination into a Connection.*
    e.g. MySql -> CSV

    An invocation of `trigger_sync` will attempt to start a sync job for
    the specified `connection_id` representing the Connection in
    Airbyte.

    `trigger_sync` will poll Airbyte Server for the Connection status and
    will only complete when the sync has completed or
    when it receives an error status code from an API call.

    As of `prefect-airbyte==0.1.3`, the kwargs `airbyte_server_host` and
    `airbyte_server_port` can be replaced by passing an `airbyte_server` block
    instance to generate the `AirbyteClient`. Using the `airbyte_server` block is
    preferred, but the individual kwargs remain for backwards compatibility.

    Args:
        connection_id: Airbyte connection ID to trigger a sync for.
        airbyte_server: An `AirbyteServer` block to create an `AirbyteClient`.
        airbyte_server_host: Airbyte server host to connect to.
        airbyte_server_port: Airbyte server port to connect to.
        airbyte_api_version: Airbyte API version to use.
        poll_interval_s: How often to poll Airbyte for sync status.
        status_updates: Whether to log sync job status while polling.
        timeout: The POST request `timeout` for the `httpx.AsyncClient`.
    Raises:
        ValueError: If `connection_id` is not a valid UUID.
        AirbyteSyncJobFailed: If airbyte returns `JOB_STATUS_FAILED`.
        AirbyteConnectionInactiveException: If a given connection is inactive.
        AirbyeConnectionDeprecatedException: If a given connection is deprecated.
    Returns:
        Job metadata, including the connection ID and final status of the sync.
    Examples:
        Flow that triggers an Airybte connection sync:
        ```python
        from prefect import flow
        from prefect_airbyte.connections import trigger_sync
        from prefect_airbyte.server import AirbyteServer
        @flow
        def example_trigger_sync_flow():
            # Run other tasks and subflows here
            trigger_sync(
                airbyte_server=AirbyteServer.load("oss-airbyte"),
                connection_id="your-connection-id-to-sync"
            )
        example_trigger_sync_flow()
        ```
    """
    logger = get_run_logger()

    if not airbyte_server:
        warn(
            "The use of `airbyte_server_host`, `airbyte_server_port`, and "
            "`airbyte_api_version` is deprecated and will be removed in a "
            "future release. Please pass an `airbyte_server` block to this "
            "task instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if any([airbyte_server_host, airbyte_server_port, airbyte_api_version]):
            airbyte_server = AirbyteServer(
                server_host=airbyte_server_host or "localhost",
                server_port=airbyte_server_port or 8000,
                api_version=airbyte_api_version or "v1",
            )
        else:
            airbyte_server = AirbyteServer()
    else:
        if any([airbyte_server_host, airbyte_server_port, airbyte_api_version]):
            logger.info(
                "Ignoring `airbyte_server_host`, `airbyte_api_version`, "
                "and `airbyte_server_port` because `airbyte_server` block "
                " was passed. Using API URL from `airbyte_server` block: "
                f"{airbyte_server.base_url!r}."
            )

    try:
        uuid.UUID(connection_id)
    except (TypeError, ValueError):
        raise ValueError(
            "Parameter `connection_id` *must* be a valid UUID \
            i.e. 32 hex characters, including hyphens."
        )

    async with airbyte_server.get_client(
        logger=logger, timeout=timeout
    ) as airbyte_client:

        logger.info(
            f"Getting Airbyte Connection {connection_id}, poll interval "
            f"{poll_interval_s} seconds, airbyte_base_url {airbyte_server.base_url}"
        )

        connection_status = await airbyte_client.get_connection_status(connection_id)

        if connection_status == CONNECTION_STATUS_ACTIVE:
            # Trigger manual sync on the Connection ...
            (
                job_id,
                job_created_at,
            ) = await airbyte_client.trigger_manual_sync_connection(connection_id)

            job_status = JOB_STATUS_PENDING

            while job_status not in terminal_job_statuses:
                (
                    job_status,
                    job_created_at,
                    job_updated_at,
                ) = await airbyte_client.get_job_status(job_id)

                # pending┃running┃incomplete┃failed┃succeeded┃cancelled
                if job_status == JOB_STATUS_SUCCEEDED:
                    logger.info(f"Job {job_id} succeeded.")
                elif job_status in [JOB_STATUS_FAILED, JOB_STATUS_CANCELLED]:
                    logger.error(f"Job {job_id} {job_status}.")
                    raise err.AirbyteSyncJobFailed(f"Job {job_id} {job_status}.")
                else:
                    if status_updates:
                        logger.info(job_status)
                    # wait for next poll interval
                    await sleep(poll_interval_s)

            return {
                "connection_id": connection_id,
                "status": connection_status,
                "job_status": job_status,
                "job_created_at": job_created_at,
                "job_updated_at": job_updated_at,
            }
        elif connection_status == CONNECTION_STATUS_INACTIVE:
            logger.error(
                f"Connection: {connection_id} is inactive"
                " - you'll need to enable it in your Airbyte instance"
            )
            raise err.AirbyteConnectionInactiveException(
                f"Please enable the Connection {connection_id} in Airbyte instance."
            )
        elif connection_status == CONNECTION_STATUS_DEPRECATED:
            logger.error(f"Connection {connection_id} is deprecated.")
            raise err.AirbyeConnectionDeprecatedException(
                f"Connection {connection_id} is deprecated."
            )


# The below implements the `JobBlock` version of the above `trigger_sync` task.


class AirbyteSyncResult(BaseModel):
    """Model representing a result from an `AirbyteSync` job run."""

    created_at: datetime
    job_status: Literal["succeeded", "failed", "pending", "cancelled"]
    job_id: int
    records_synced: int
    updated_at: datetime


class AirbyteSync(JobRun):
    """A `JobRun` representing an Airbyte sync job."""

    def __init__(self, airbyte_connection: "AirbyteConnection", job_id: int):
        self.airbyte_connection: "AirbyteConnection" = airbyte_connection
        self.job_id: int = job_id
        self._records_synced: int = 0

    @sync_compatible
    async def wait_for_completion(self):
        """Wait for the `AirbyteConnection` sync to reach a terminal state.

        Raises:
            AirbyteSyncJobFailed: If the sync job fails.
        """
        async with self.airbyte_connection.airbyte_server.get_client(
            logger=self.airbyte_connection.logger,
            timeout=self.airbyte_connection.timeout,
        ) as airbyte_client:

            job_status = JOB_STATUS_PENDING

            while job_status not in terminal_job_statuses:
                job_info = await airbyte_client.get_job_info(self.job_id)

                job_status = job_info["job"]["status"]

                self._records_synced = job_info["attempts"][-1]["attempt"].get(
                    "recordsSynced", 0
                )

                # pending┃running┃failed┃succeeded┃cancelled
                if job_status == JOB_STATUS_SUCCEEDED:
                    self.logger.info(f"Job {self.job_id} succeeded.")
                elif job_status in [JOB_STATUS_FAILED, JOB_STATUS_CANCELLED]:
                    self.logger.error(f"Job {self.job_id} {job_status}.")
                    raise err.AirbyteSyncJobFailed(f"Job {self.job_id} {job_status}.")
                else:
                    if self.airbyte_connection.status_updates:
                        self.logger.info(job_status)
                    # wait for next poll interval
                    await sleep(self.airbyte_connection.poll_interval_s)

    @sync_compatible
    async def fetch_result(self) -> AirbyteSyncResult:
        """Fetch the result of the `AirbyteSync`.

        Returns:
            `AirbyteSyncResult`: object containing metadata for the `AirbyteSync`.
        """
        async with self.airbyte_connection.airbyte_server.get_client(
            logger=self.airbyte_connection.logger,
            timeout=self.airbyte_connection.timeout,
        ) as airbyte_client:
            job_info = await airbyte_client.get_job_info(self.job_id)

            job_status = job_info["job"]["status"]
            job_created_at = job_info["job"]["createdAt"]
            job_updated_at = job_info["job"]["updatedAt"]

            return AirbyteSyncResult(
                created_at=job_created_at,
                job_id=self.job_id,
                job_status=job_status,
                records_synced=self._records_synced,
                updated_at=job_updated_at,
            )


class AirbyteConnection(JobBlock):
    """A block representing an existing Airbyte connection.

    Attributes:
        airbyte_server: `AirbyteServer` block representing the Airbyte instance
            where the Airbyte connection is defined.
        connection_id: UUID of the Airbyte Connection to trigger.
        poll_interval_s: Time in seconds between status checks of the Airbyte sync job.
        status_updates: Whether to log job status on each poll of the Airbyte sync job.
        timeout: Timeout in seconds for requests made by `httpx.AsyncClient`.

    Examples:
        Load an existing `AirbyteConnection` block:
        ```python
        from prefect_airbyte import AirbyteConnection

        airbyte_connection = AirbyteConnection.load("BLOCK_NAME")
        ```

        Run an Airbyte connection sync as a flow:
        ```python
        from prefect import flow
        from prefect_airbyte import AirbyteConnection
        from prefect_airbyte.flows import run_connection_sync # this is a flow

        airbyte_connection = AirbyteConnection.load("BLOCK_NAME")

        @flow
        def airbyte_orchestrator():
            run_connection_sync(airbyte_connection) # now it's a subflow
        ```
    """

    _block_type_name = "Airbyte Connection"
    _logo_url = "https://cdn.sanity.io/images/3ugk85nk/production/7f50097d1915fe75b0ee84c951c742a83d3c53cb-250x250.png"  # noqa: E501
    _documentation_url = "https://prefecthq.github.io/prefect-airbyte/connections/#prefect_airbyte.connections.AirbyteConnection"  # noqa

    airbyte_server: AirbyteServer = Field(
        default=...,
        description=(
            "AirbyteServer block representing the Airbyte instance "
            "where the Airbyte connection is defined."
        ),
    )

    connection_id: uuid.UUID = Field(
        default=...,
        description="UUID of the Airbyte Connection to trigger.",
    )

    poll_interval_s: int = Field(
        default=15,
        description="Time in seconds between status checks of the Airbyte sync job.",
    )

    status_updates: bool = Field(
        default=False,
        description="Whether to log job status on each poll of the Airbyte sync job.",
    )

    timeout: int = Field(
        default=5,
        description="Timeout in seconds for requests made by httpx.AsyncClient.",
    )

    @sync_compatible
    async def trigger(self) -> AirbyteSync:
        """Trigger a sync of the defined Airbyte connection.

        Returns:
            An `AirbyteSync` `JobRun` object representing the active sync job.

        Raises:
            AirbyteConnectionInactiveException: If the connection is inactive.
            AirbyteConnectionDeprecatedException: If the connection is deprecated.
        """
        str_connection_id = str(self.connection_id)

        async with self.airbyte_server.get_client(
            logger=self.logger, timeout=self.timeout
        ) as airbyte_client:

            self.logger.info(
                f"Triggering Airbyte Connection {self.connection_id}, "
                f"in workspace at {self.airbyte_server.base_url!r}"
            )

            connection_status = await airbyte_client.get_connection_status(
                str_connection_id
            )

            if connection_status == CONNECTION_STATUS_ACTIVE:
                (job_id, _,) = await airbyte_client.trigger_manual_sync_connection(
                    str_connection_id
                )

                return AirbyteSync(
                    airbyte_connection=self,
                    job_id=job_id,
                )

            elif connection_status == CONNECTION_STATUS_INACTIVE:
                raise err.AirbyteConnectionInactiveException(
                    f"Connection: {self.connection_id!r} is inactive"
                    f"Please enable the connection {self.connection_id!r} "
                    "in your Airbyte instance."
                )
            elif connection_status == CONNECTION_STATUS_DEPRECATED:
                raise err.AirbyeConnectionDeprecatedException(
                    f"Connection {self.connection_id!r} is deprecated."
                )
