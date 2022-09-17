"""Client for interacting with Airbyte instance"""

import logging
from typing import Tuple

import httpx

from prefect_airbyte import exceptions as err


class AirbyteClient:
    """
    Client class used to call API endpoints on an Airbyte server.

    This client assumes that you're using an OSS Airbyte server which does not require
    an API key to access its a API.

    For more info, see the [Airbyte docs](https://docs.airbyte.io/api-documentation).

    Attributes:
        logger: A logger instance used by the client to log messages related to
            API calls.
        airbyte_base_url str: Base API endpoint URL for Airbyte.
        timeout: The number of seconds to wait before an API call times out.
    """

    def __init__(
        self,
        logger: logging.Logger,
        airbyte_base_url: str = "http://localhost:8000/api/v1",
        timeout: int = 5,
    ):
        self.airbyte_base_url = airbyte_base_url
        self.logger = logger
        self.timeout = timeout

    async def _establish_session(self) -> httpx.AsyncClient:
        """
        Checks health of the Airbyte server and establishes a session.

        Returns:
            Session used to communicate with the Airbyte API.
        """
        client = httpx.AsyncClient(timeout=self.timeout)
        await self.check_health_status(client)
        return client

    async def check_health_status(self, client: httpx.AsyncClient) -> bool:
        """
        Checks the health status of an AirbyteInstance.

        Args:
            Session used to interact with the Airbyte API.

        Returns:
            True if the server is healthy. False otherwise.
        """
        get_connection_url = self.airbyte_base_url + "/health/"
        try:
            response = await client.get(get_connection_url)
            response.raise_for_status()

            self.logger.debug("Health check response: %s", response.json())
            key = "available" if "available" in response.json() else "db"
            health_status = response.json()[key]
            if not health_status:
                raise err.AirbyteServerNotHealthyException(
                    f"Airbyte Server health status: {health_status}"
                )
            return True
        except httpx.HTTPStatusError as e:
            raise err.AirbyteServerNotHealthyException() from e

    async def create_client(self) -> httpx.AsyncClient:
        """
        Convenience method for establishing a healthy session with the Airbyte server.

        Returns:
            Session for interacting with the Airbyte server.
        """
        client = await self._establish_session()
        return client

    async def export_configuration(
        self,
    ) -> bytes:
        """
        Triggers an export of Airbyte configuration.

        Returns:
            Gzipped Airbyte configuration data.
        """
        client = await self.create_client()

        get_connection_url = self.airbyte_base_url + "/deployment/export/"

        try:
            response = await client.post(get_connection_url)
            response.raise_for_status()

            self.logger.debug("Export configuration response: %s", response)

            export_config = response.content
            return export_config
        except httpx.HTTPStatusError as e:
            raise err.AirbyteExportConfigurationFailed() from e

    async def get_connection_status(self, connection_id: str) -> str:
        """
        Gets the status of a defined Airbyte connection.

        Args:
            connection_id: ID of an existing Airbyte connection.

        Returns:
            The status of the defined Airbyte connection.
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
                raise err.ConnectionNotFoundException() from e
            else:
                raise err.AirbyteServerNotHealthyException() from e

    async def trigger_manual_sync_connection(
        self, connection_id: str
    ) -> Tuple[str, str]:
        """
        Triggers a manual sync of the connection.

        Args:
            connection_id: ID of connection to sync.

        Returns:
            job_id: ID of the job that was triggered.
            created_at: Datetime string of when the job was created.

        """
        client = await self.create_client()

        get_connection_url = self.airbyte_base_url + "/connections/sync/"

        # TODO - no current authentication methods from Airbyte
        try:
            response = await client.post(
                get_connection_url, json={"connectionId": connection_id}
            )
            response.raise_for_status()
            job = response.json()["job"]
            job_id = job["id"]
            job_created_at = job["createdAt"]
            return job_id, job_created_at
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise err.ConnectionNotFoundException(
                    f"Connection {connection_id} not found, please double "
                    f"check the connection_id."
                ) from e

            raise err.AirbyteServerNotHealthyException() from e

    async def get_job_status(
        self, job_id: str, stream_logs: bool = False
    ) -> Tuple[str, str, str]:
        """
        Gets the status of an Airbyte connection sync job.

        Args:
            job_id: ID of the Airbyte job to check.
            stream_logs: whether to capture Airbyte job logs in metadata.

        Returns:
            job_metadata
        """
        client = await self.create_client()

        get_connection_url = self.airbyte_base_url + "/jobs/get/"
        try:
            response = await client.post(get_connection_url, json={"id": job_id})
            response.raise_for_status()

            raw_job = response.json()["job"]
            attempts = response.json()["attempts"]

            job_info = {
                field: raw_job[field] for field in ["status", "createdAt", "updatedAt"]
            }

            if stream_logs and attempts:
                # always grab logs for most recent attempt of this job
                job_info["logs"] = attempts[0]["logs"]["logLines"]
                job_info["n_log_lines"] = len(job_info["logs"])
            else:
                job_info["logs"] = None

            return job_info

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise err.JobNotFoundException(f"Job {job_id} not found.") from e
            raise err.AirbyteServerNotHealthyException() from e
