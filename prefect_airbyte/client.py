"""Client for interacting with Airbyte instance"""

import logging, requests
from requests import RequestException

from prefect_airbyte import exceptions as err

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
        airbyte_base_url str: base api endpoint url for airbyte

    Returns:
        AirbyteClient: an instance of AirbyteClient
    """

    def __init__(self, logger, airbyte_base_url: str = "http://localhost:8000/api/v1"):
        self.airbyte_base_url = airbyte_base_url
        self.logger = logger

    def establish_session(self):
        session = requests.Session()
        if self.check_health_status(session):
            return session

    def check_health_status(self, session):
        get_connection_url = self.airbyte_base_url + "/health/"
        try:
            response = session.get(get_connection_url)
            self.logger.debug("Health check response: %s", response.json())
            key = "available" if "available" in response.json() else "db"
            health_status = response.json()[key]
            if not health_status:
                raise err.AirbyteServerNotHealthyException(
                    f"Airbyte Server health status: {health_status}"
                )
            return True
        except RequestException as e:
            raise err.AirbyteServerNotHealthyException(e)

    def export_configuration(
        self,
        airbyte_base_url: str,
        session: requests.Session,
    ) -> bytearray:
        """
        Trigger an export of Airbyte configuration &mdash; [see the airbyte docs for `/v1/deployment/export`](https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html#post-/v1/deployment/export).

        Args:
            airbyte_base_url: URL of Airbyte server.
            session: requests session with which to make call to the Airbyte server
            logger: task logger

        Returns:
            - byte array of Airbyte configuration data
        """
        get_connection_url = airbyte_base_url + "/deployment/export/"

        try:
            response = session.post(get_connection_url)
            if response.status_code == 200:
                self.logger.debug("Export configuration response: %s", response)
                export_config = response.content
                return export_config
        except RequestException as e:
            raise err.AirbyteExportConfigurationFailed(e)
        
    def get_connection_status(self, session, airbyte_base_url, connection_id):
        get_connection_url = airbyte_base_url + "/connections/get/"

        # TODO - Missing authentication because Airbyte servers currently do not support authentication
        try:
            response = session.post(
                get_connection_url, json={"connectionId": connection_id}
            )

            response.raise_for_status()

            connection_status = response.json()["status"]
            return connection_status
        except RequestException as e:
            raise err.AirbyteServerNotHealthyException(e)


    def trigger_manual_sync_connection(self, session, airbyte_base_url, connection_id):
        """
        Trigger a manual sync of the Connection &mdash; [see the airbyte docs for /v1/connections/sync](https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html#post-/v1/connections/sync).

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
                job_id = response.json()["job"]["id"]
                job_created_at = response.json()["job"]["createdAt"]
                return job_id, job_created_at
            elif response.status_code == 404:
                # connection_id not found
                self.logger.warning(
                    f"Connection {connection_id} not found, please double "
                    f"check the connection_id ..."
                )
                raise err.ConnectionNotFoundException(
                    f"Connection {connection_id} not found, please double "
                    f"check the connection_id ..."
                )
        except RequestException as e:
            raise err.AirbyteServerNotHealthyException(e)

    def get_job_status(self, session, airbyte_base_url, job_id):
        get_connection_url = airbyte_base_url + "/jobs/get/"
        try:
            response = session.post(get_connection_url, json={"id": job_id})
            if response.status_code == 200:
                job_status = response.json()["job"]["status"]
                job_created_at = response.json()["job"]["createdAt"]
                job_updated_at = response.json()["job"]["updatedAt"]
                return job_status, job_created_at, job_updated_at
            elif response.status_code == 404:
                self.logger.error(f"Job {job_id} not found...")
                raise err.JobNotFoundException(f"Job {job_id} not found...")
        except RequestException as e:
            raise err.AirbyteServerNotHealthyException(e)