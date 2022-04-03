from time import sleep
import uuid

import requests
from requests import RequestException

from prefect import task
from prefect.logging.loggers import get_logger


# Connection statuses
CONNECTION_STATUS_ACTIVE = "active"
CONNECTION_STATUS_INACTIVE = "inactive"
CONNECTION_STATUS_DEPRECATED = "deprecated"

# Job statuses
JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_PENDING = "pending"
    
class ConnectionNotFoundException(Exception):
    pass


class AirbyteServerNotHealthyException(Exception):
    pass


class JobNotFoundException(Exception):
    pass


class AirbyteSyncJobFailed(Exception):
    pass


class AirbyteExportConfigurationFailed(Exception):
    pass


class AirbyteClient:
    """
    Esablishes a session with an Airbyte instance and evaluates its current health
    status.

    This client assumes that you're using Airbyte Open-Source, since "For
    Airbyte Open-Source you don't need the API Token for
    Authentication! All endpoints are possible to access using the
    API without it."
    For more information refer to the [Airbyte docs](https://docs.airbyte.io/api-documentation).

    Args:
        - airbyte_base_url (str, mandatory): base api endpoint url for airbyte.
          ex. http://localhost:8000/api/v1

    Returns:
        - session connection with Airbyte
    """

    def __init__(self, logger, airbyte_base_url: str):
        self.airbyte_base_url = airbyte_base_url
        self.logger = logger

    def _establish_session(self):
        session = requests.Session()
        if self._check_health_status(session):
            return session

    def _check_health_status(self, session):
        get_connection_url = self.airbyte_base_url + "/health/"
        try:
            response = session.get(get_connection_url)
            self.logger.debug("Health check response: %s", response.json())
            key = "available" if "available" in response.json() else "db"
            health_status = response.json()[key]
            if not health_status:
                raise AirbyteServerNotHealthyException(
                    f"Airbyte Server health status: {health_status}"
                )
            return True
        except RequestException as e:
            raise AirbyteServerNotHealthyException(e)

    def export_configuration(airbyte_base_url, session, logger) -> bytearray:
        """
        Trigger an export of Airbyte configuration, see:
        https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html#post-/v1/deployment/export

        Args:
            - airbyte_base_url: URL of Airbyte server.
            - session: requests session with which to make call to the Airbyte server
            - logger: task logger

        Returns:
            - byte array of Airbyte configuration data
        """
        get_connection_url = airbyte_base_url + "/deployment/export/"

        try:
            response = session.post(get_connection_url)
            if response.status_code == 200:
                logger.debug("Export configuration response: %s", response)
                export_config = response.content
                return export_config
        except RequestException as e:
            raise AirbyteExportConfigurationFailed(e)
        
def get_connection_status(logger, session, airbyte_base_url, connection_id):
    get_connection_url = airbyte_base_url + "/connections/get/"

    # TODO - Missing authentication because Airbyte servers currently do not support authentication
    try:
        response = session.post(
            get_connection_url, json={"connectionId": connection_id}
        )
        logger.info(response.json())

        response.raise_for_status()

        # check whether a schedule exists ...
        schedule = response.json()["schedule"]
        if schedule:
            logger.warning("Found existing Connection schedule, removing ...")

            # mandatory fields for Connection update ...
            sync_catalog = response.json()["syncCatalog"]
            connection_status = response.json()["status"]

            update_connection_url = airbyte_base_url + "/connections" "/update/"
            response2 = session.post(
                update_connection_url,
                json={
                    "connectionId": connection_id,
                    "syncCatalog": sync_catalog,
                    "schedule": None,
                    "status": connection_status,
                },
            )
            logger.info(response2.json())

            if response2.status_code == 200:
                logger.info("Schedule removed ok.")
            else:
                logger.warning("Schedule not removed.")

        connection_status = response.json()["status"]
        return connection_status
    except RequestException as e:
        raise AirbyteServerNotHealthyException(e)


def trigger_manual_sync_connection(logger, session, airbyte_base_url, connection_id):
    """
    Trigger a manual sync of the Connection, see:
    https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc
    -api-docs.html#post-/v1/connections/sync

    Args:
        session: requests session with which to make call to Airbyte server
        airbyte_base_url: URL of Airbyte server
        connection_id: ID of connection to sync

    Returns: created_at - timestamp of sync job creation

    """
    get_connection_url = airbyte_base_url + "/connections/sync/"

    # TODO - missing authentication ...
    try:
        response = session.post(
            get_connection_url, json={"connectionId": connection_id}
        )
        if response.status_code == 200:
            logger.info(response.json())
            job_id = response.json()["job"]["id"]
            job_created_at = response.json()["job"]["createdAt"]
            return job_id, job_created_at
        elif response.status_code == 404:
            # connection_id not found
            logger.warning(
                f"Connection {connection_id} not found, please double "
                f"check the connection_id ..."
            )
            raise ConnectionNotFoundException(
                f"Connection {connection_id} not found, please double "
                f"check the connection_id ..."
            )
    except RequestException as e:
        raise AirbyteServerNotHealthyException(e)

def get_job_status(logger, session, airbyte_base_url, job_id):
    get_connection_url = airbyte_base_url + "/jobs/get/"
    try:
        response = session.post(get_connection_url, json={"id": job_id})
        if response.status_code == 200:
            logger.info(response.json())
            job_status = response.json()["job"]["status"]
            job_created_at = response.json()["job"]["createdAt"]
            job_updated_at = response.json()["job"]["updatedAt"]
            return job_status, job_created_at, job_updated_at
        elif response.status_code == 404:
            logger.error(f"Job {job_id} not found...")
            raise JobNotFoundException(f"Job {job_id} not found...")
    except RequestException as e:
        raise AirbyteServerNotHealthyException(e)

@task
async def trigger_sync(
    airbyte_server_host: str = "localhost",
    airbyte_server_port: int = "8000",
    airbyte_api_version: str = "v1",
    connection_id: str = None,
    poll_interval_s: int = 15,
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
        - airbyte_server_host (str, optional): Hostname of Airbyte server where connection is
            configured. Will overwrite the value provided at init if provided.
        - airbyte_server_port (str, optional): Port that the Airbyte server is listening on.
            Will overwrite the value provided at init if provided.
        - airbyte_api_version (str, optional): Version of Airbyte API to use to trigger connection
            sync. Will overwrite the value provided at init if provided.
        - connection_id (str, optional): if provided,
            will overwrite the value provided at init.
        - poll_interval_s (int, optional): this task polls the
            Airbyte API for status, if provided this value will
            override the default polling time of 15 seconds.

    Returns:
        - dict: connection_id (str) and succeeded_at (timestamp str)
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
    session = airbyte._establish_session()

    logger.info(
        f"Getting Airbyte Connection {connection_id}, poll interval "
        f"{poll_interval_s} seconds, airbyte_base_url {airbyte_base_url}"
    )

    connection_status = get_connection_status(
        logger, session, airbyte_base_url, connection_id
    )
    if connection_status == CONNECTION_STATUS_ACTIVE:
        # Trigger manual sync on the Connection ...
        job_id, job_created_at = trigger_manual_sync_connection(
            logger, session, airbyte_base_url, connection_id
        )

        job_status = JOB_STATUS_PENDING

        while job_status not in [JOB_STATUS_FAILED, JOB_STATUS_SUCCEEDED]:
            job_status, job_created_at, job_updated_at = get_job_status(
                logger, session, airbyte_base_url, job_id
            )

            # pending┃running┃incomplete┃failed┃succeeded┃cancelled
            if job_status == JOB_STATUS_SUCCEEDED:
                logger.info(f"Job {job_id} succeeded.")
            elif job_status == JOB_STATUS_FAILED:
                logger.error(f"Job {job_id} failed.")
                raise AirbyteSyncJobFailed(f"Job {job_id} failed.")
            else:
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
        raise AirbyteServerNotHealthyException(
            f"Please enable the Connection {connection_id} in Airbyte Server."
        )
    elif connection_status == CONNECTION_STATUS_DEPRECATED:
        logger.error(f"Connection {connection_id} is deprecated.")
        raise AirbyteServerNotHealthyException(f"Connection {connection_id} is deprecated.")