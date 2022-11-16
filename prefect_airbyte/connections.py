"""Tasks for connecting to Airbyte and triggering connection syncs"""
import uuid
from asyncio import sleep
from typing import Optional
from warnings import warn

from prefect import get_run_logger, task

from prefect_airbyte import exceptions as err
from prefect_airbyte.server import AirbyteServer

# Connection statuses
CONNECTION_STATUS_ACTIVE = "active"
CONNECTION_STATUS_INACTIVE = "inactive"
CONNECTION_STATUS_DEPRECATED = "deprecated"

# Job statuses
JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_PENDING = "pending"


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
) -> dict:
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
        err.AirbyteSyncJobFailed: If airbyte returns `JOB_STATUS_FAILED`.
        err.AirbyteConnectionInactiveException: If a given connection is inactive.
        err.AirbyeConnectionDeprecatedException: If a given connection is deprecated.

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

            while job_status not in [JOB_STATUS_FAILED, JOB_STATUS_SUCCEEDED]:
                (
                    job_status,
                    job_created_at,
                    job_updated_at,
                ) = await airbyte_client.get_job_status(job_id)

                # pending┃running┃incomplete┃failed┃succeeded┃cancelled
                if job_status == JOB_STATUS_SUCCEEDED:
                    logger.info(f"Job {job_id} succeeded.")
                elif job_status == JOB_STATUS_FAILED:
                    logger.error(f"Job {job_id} failed.")
                    raise err.AirbyteSyncJobFailed(f"Job {job_id} failed.")
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
