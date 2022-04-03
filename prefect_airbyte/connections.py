"""Tasks for connecting to Airbyte and triggering connection syncs"""
from time import sleep
import uuid

from prefect import task
from prefect.logging.loggers import get_logger
from prefect_airbyte.client import AirbyteClient
from prefect_airbyte import exceptions as err

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
    airbyte_server_host: str = "localhost",
    airbyte_server_port: int = "8000",
    airbyte_api_version: str = "v1",
    connection_id: str = None,
    poll_interval_s: int = 15,
    status_updates: bool = False
) -> dict:
    """
    Task run method for triggering an Airbyte Connection.

    *It is assumed that the user will have previously configured
    a Source & Destination into a Connection.*
    e.g. MySql -> CSV

    An invocation of `run` will attempt to start a sync job for
    the specified `connection_id` representing the Connection in
    Airbyte.

    `run` will poll Airbyte Server for the Connection status and
    will only complete when the sync has completed or
    when it receives an error status code from an API call.

    Args:
        str airbyte_server_host : Hostname of Airbyte server where connection is
            configured. Will overwrite the value provided at init if provided.
        str airbyte_server_port: Port that the Airbyte server is listening on.
            Will overwrite the value provided at init if provided.
        str airbyte_api_version: Version of Airbyte API to use to trigger connection
            sync. Will overwrite the value provided at init if provided.
        str connection_id: if provided,
            will overwrite the value provided at init.
        int poll_interval_s: this task polls the
            Airbyte API for status, if provided this value will
            override the default polling time of 15 seconds.
        bool status_updates: whether to log status as the task polls jobs

    Returns:
        dict: connection_id (str) and succeeded_at (timestamp str)
        
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

    if not connection_id:
        raise ValueError(
            "Value for parameter `connection_id` *must* \
        be provided."
        )

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

    airbyte = AirbyteClient(logger, airbyte_base_url)
    session = airbyte.establish_session()

    logger.info(
        f"Getting Airbyte Connection {connection_id}, poll interval "
        f"{poll_interval_s} seconds, airbyte_base_url {airbyte_base_url}"
    )

    connection_status = airbyte.get_connection_status(
        session, airbyte_base_url, connection_id
    )
    if connection_status == CONNECTION_STATUS_ACTIVE:
        # Trigger manual sync on the Connection ...
        job_id, job_created_at = airbyte.trigger_manual_sync_connection(
            session, airbyte_base_url, connection_id
        )

        job_status = JOB_STATUS_PENDING

        while job_status not in [JOB_STATUS_FAILED, JOB_STATUS_SUCCEEDED]:
            job_status, job_created_at, job_updated_at = airbyte.get_job_status(
                session, airbyte_base_url, job_id
            )

            # pending┃running┃incomplete┃failed┃succeeded┃cancelled
            if job_status == JOB_STATUS_SUCCEEDED:
                logger.info(f"Job {job_id} succeeded.")
            elif job_status == JOB_STATUS_FAILED:
                logger.error(f"Job {job_id} failed.")
                raise err.AirbyteSyncJobFailed(f"Job {job_id} failed.")
            else:
                if status_updates: logger.info(job_status)
                # wait for next poll interval
                sleep(poll_interval_s)

        return {
            "connection_id": connection_id,
            "status": connection_status,
            "job_status": job_status,
            "job_created_at": job_created_at,
            "job_updated_at": job_updated_at,
        }
    elif connection_status == CONNECTION_STATUS_INACTIVE:
        logger.error(
            f"Please enable the Connection {connection_id} in Airbyte Server."
        )
        raise err.AirbyteServerNotHealthyException(
            f"Please enable the Connection {connection_id} in Airbyte Server."
        )
    elif connection_status == CONNECTION_STATUS_DEPRECATED:
        logger.error(f"Connection {connection_id} is deprecated.")
        raise err.AirbyteServerNotHealthyException(f"Connection {connection_id} is deprecated.")