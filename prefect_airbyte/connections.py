"""Tasks for connecting to Airbyte and triggering connection syncs"""
import uuid
from asyncio import sleep

from prefect import task
from prefect.logging.loggers import get_logger

from prefect_airbyte import exceptions as err
from prefect_airbyte.client import AirbyteClient

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
    airbyte_server_host: str = "localhost",
    airbyte_server_port: int = "8000",
    airbyte_api_version: str = "v1",
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

    Args:
        connection_id: Airbyte connection ID to trigger a sync for.
        airbyte_server_host: Airbyte instance hostname where connection is configured.
        airbyte_server_port: Port where Airbyte instance is listening.
        airbyte_api_version: Version of Airbyte API to use to trigger connection sync.
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


        @flow
        def example_trigger_sync_flow():

            # Run other tasks and subflows here

            trigger_sync(
                connection_id="your-connection-id-to-sync"
            )

        example_trigger_sync_flow()
        ```
    """
    logger = get_logger()

    try:
        uuid.UUID(connection_id)
    except (TypeError, ValueError):
        raise ValueError(
            "Parameter `connection_id` *must* be a valid UUID \
            i.e. 32 hex characters, including hyphens."
        )

    # see https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com
    # /rapidoc-api-docs.html#overview
    airbyte_base_url = (
        f"http://{airbyte_server_host}:"
        f"{airbyte_server_port}/api/{airbyte_api_version}"
    )

    airbyte = AirbyteClient(logger, airbyte_base_url, timeout=timeout)

    logger.info(
        f"Getting Airbyte Connection {connection_id}, poll interval "
        f"{poll_interval_s} seconds, airbyte_base_url {airbyte_base_url}"
    )

    connection_status = await airbyte.get_connection_status(connection_id)

    if connection_status == CONNECTION_STATUS_ACTIVE:
        # Trigger manual sync on the Connection ...
        job_id, job_created_at = await airbyte.trigger_manual_sync_connection(
            connection_id
        )

        job_status = JOB_STATUS_PENDING

        while job_status not in [JOB_STATUS_FAILED, JOB_STATUS_SUCCEEDED]:
            job_status, job_created_at, job_updated_at = await airbyte.get_job_status(
                job_id
            )

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
