"""Client for interacting with Airbyte instance"""

import logging
from typing import Tuple

import httpx

from prefect_airbyte import exceptions as err


class AirbyteClient:
    """
    Esablishes a client session with an Airbyte instance and evaluates its current
    health status.

    This client assumes that you're using Airbyte Open-Source, since "For
    Airbyte Open-Source you don't need the API Token for
    Authentication! All endpoints are accessible using the
    API without it."
    For more info, see the [Airbyte docs](https://docs.airbyte.io/api-documentation).

    Args:
        airbyte_base_url str: base api endpoint url for airbyte

    Returns:
        AirbyteClient: an instance of AirbyteClient
    """

    def __init__(
        self,
        logger: logging.Logger,
        airbyte_base_url: str = "http://localhost:8000/api/v1",
        timeout: int = 5,
    ) -> None:
        """
        `AirbyteClient` constructor

        Args:
            logger: for client use, e.g. `prefect.logging.loggers.get_logger`
            airbyte_base_url: Full Airbyte API endpoint
            timeout: seconds for httpx client timeout

        Returns:
            AirbyteClient: an instance of the `AirbyteClient` class
        """
        self.airbyte_base_url = airbyte_base_url
        self.logger = logger
        self.timeout = timeout

    async def _establish_session(self) -> httpx.AsyncClient:
        """
        AirbyteClient method to `check_health_status` and establish a `client` session

        Returns:
            client: `httpx.AsyncClient` used to communicate with the Airbyte API
        """
        client = httpx.AsyncClient(timeout=self.timeout)
        if await self.check_health_status(client):
            return client
        else:
            raise err.AirbyteServerNotHealthyException

    async def check_health_status(self, client: httpx.AsyncClient):
        """
        Check the health status of an AirbyteInstance

        Args:
            client: `httpx.AsyncClient` instance used to interact with the Airbyte API

        Returns:
            bool: representing whether the server is healthy
        """
        get_connection_url = self.airbyte_base_url + "/health/"
        try:
            response = await client.get(get_connection_url)
            self.logger.debug("Health check response: %s", response.json())
            key = "available" if "available" in response.json() else "db"
            health_status = response.json()[key]
            if not health_status:
                raise err.AirbyteServerNotHealthyException(
                    f"Airbyte Server health status: {health_status}"
                )
            return True
        except httpx.HTTPStatusError as e:
            raise err.AirbyteServerNotHealthyException(e)

    async def create_client(self) -> httpx.AsyncClient:
        """
        Convenience method for establishing a healthy `httpx` Airbyte client

        Args:
            timeout: `int` seconds for request timeout with this client
        Returns:
            httpx.AsyncClient: client for interacting with Airbyte instance
        """
        client = await self._establish_session()
        return client

    async def export_configuration(
        self,
    ) -> bytearray:
        """
        Trigger an export of Airbyte configuration

        Args:
            client: httpx client with which to make call to the Airbyte server

        Returns:
            byte array of Airbyte configuration data
        """
        client = await self.create_client()

        get_connection_url = self.airbyte_base_url + "/deployment/export/"

        try:
            response = await client.post(get_connection_url)
            if response.status_code == 200:
                self.logger.debug("Export configuration response: %s", response)
                export_config = response.content
                return export_config
        except httpx.HTTPStatusError as e:
            raise err.AirbyteExportConfigurationFailed(e)

    async def get_connection_status(self, connection_id: str) -> str:
        """
        Get the status of a defined Airbyte connection

        Args:
            connection_id: string value of the defined airbyte connection

        Returns:
            str: the status of a defined Airbyte connection
        """
        client = await self.create_client()

        get_connection_url = self.airbyte_base_url + "/connections/get/"

        # TODO - Missing auth because Airbyte API currently doesn't yet support auth
        try:
            response = await client.post(
                get_connection_url, json={"connectionId": connection_id}
            )

            response.raise_for_status()

            connection_status = response.json()["status"]
            return connection_status
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise err.ConnectionNotFoundException
            else:
                raise err.AirbyteServerNotHealthyException(e)

    async def trigger_manual_sync_connection(
        self, connection_id: str
    ) -> Tuple[str, str]:
        """
        Trigger a manual sync of the Connection

        Args:
            connection_id: ID of connection to sync

        Returns: created_at - timestamp of sync job creation

        """
        client = await self.create_client()

        get_connection_url = self.airbyte_base_url + "/connections/sync/"

        # TODO - no current authentication methods from Airbyte
        try:
            response = await client.post(
                get_connection_url, json={"connectionId": connection_id}
            )
            if response.status_code == 200:
                job_id = response.json()["job"]["id"]
                print(response.json())
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
        except httpx.HTTPStatusError as e:
            raise err.AirbyteServerNotHealthyException(e)

    async def get_job_status(self, job_id: str) -> str:
        """
        Get the status of an Airbyte connection sync job

        Args:
            job_id: str value of the airbyte job id as defined by airbyte

        Returns:
            byte array of Airbyte configuration data
        """
        client = await self.create_client()

        get_connection_url = self.airbyte_base_url + "/jobs/get/"
        try:
            response = await client.post(get_connection_url, json={"id": job_id})
            if response.status_code == 200:
                job_status = response.json()["job"]["status"]
                job_created_at = response.json()["job"]["createdAt"]
                job_updated_at = response.json()["job"]["updatedAt"]
                return job_status, job_created_at, job_updated_at
            elif response.status_code == 404:
                self.logger.error(f"Job {job_id} not found...")
                raise err.JobNotFoundException(f"Job {job_id} not found...")
        except httpx.HTTPStatusError as e:
            raise err.AirbyteServerNotHealthyException(e)
