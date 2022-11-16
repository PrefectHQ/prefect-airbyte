"""Client for interacting with Airbyte instance"""
import logging
from typing import Tuple
from warnings import warn

import httpx

from prefect_airbyte import exceptions as err


class AirbyteClient:
    """
    Client class used to call API endpoints on an Airbyte server.

    This client currently supports username/password authentication as set in `auth`.

    For more info, see the [Airbyte docs](https://docs.airbyte.io/api-documentation).

    Attributes:
        airbyte_base_url str: Base API endpoint URL for Airbyte.
        auth: Username and password for Airbyte API.
        logger: A logger instance used by the client to log messages related to
            API calls.
        timeout: The number of seconds to wait before an API call times out.
    """

    def __init__(
        self,
        logger: logging.Logger,
        airbyte_base_url: str = "http://localhost:8000/api/v1",
        auth: Tuple[str, str] = ("airbyte", "password"),
        timeout: int = 5,
    ):
        self._closed = False
        self._started = False

        self.airbyte_base_url = airbyte_base_url
        self.auth = auth
        self.logger = logger
        self.timeout = timeout
        self._client = httpx.AsyncClient(
            base_url=self.airbyte_base_url, auth=self.auth, timeout=self.timeout
        )

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

    async def export_configuration(
        self,
    ) -> bytes:
        """
        Triggers an export of Airbyte configuration.

        Returns:
            Gzipped Airbyte configuration data.
        """
        warn(
            "As of Airbyte v0.40.7-alpha, the Airbyte API no longer supports "
            "exporting configurations. See the Octavia CLI docs for more info.",
            DeprecationWarning,
            stacklevel=2,
        )

        get_connection_url = self.airbyte_base_url + "/deployment/export/"

        try:
            response = await self._client.post(get_connection_url)
            response.raise_for_status()

            self.logger.debug("Export configuration response: %s", response)

            export_config = response.content
            return export_config
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self.logger.error(
                    "If you are using Airbyte v0.40.7-alpha, there is no longer "
                    "an API endpoint for exporting configurations."
                )
                raise err.AirbyteExportConfigurationFailed() from e

    async def get_connection_status(self, connection_id: str) -> str:
        """
        Gets the status of a defined Airbyte connection.

        Args:
            connection_id: ID of an existing Airbyte connection.

        Returns:
            The status of the defined Airbyte connection.
        """
        get_connection_url = self.airbyte_base_url + "/connections/get/"

        try:
            response = await self._client.post(
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
        get_connection_url = self.airbyte_base_url + "/connections/sync/"

        try:
            response = await self._client.post(
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

    async def get_job_status(self, job_id: str) -> Tuple[str, str, str]:
        """
        Gets the status of an Airbyte connection sync job.

        Args:
            job_id: ID of the Airbyte job to check.

        Returns:
            job_status: The current status of the job.
            job_created_at: Datetime string of when the job was created.
            job_updated_at: Datetime string of the when the job was last updated.
        """
        get_connection_url = self.airbyte_base_url + "/jobs/get/"
        try:
            response = await self._client.post(get_connection_url, json={"id": job_id})
            response.raise_for_status()

            job = response.json()["job"]
            job_status = job["status"]
            job_created_at = job["createdAt"]
            job_updated_at = job["updatedAt"]
            return job_status, job_created_at, job_updated_at
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise err.JobNotFoundException(f"Job {job_id} not found.") from e
            raise err.AirbyteServerNotHealthyException() from e

    async def create_client(self) -> httpx.AsyncClient:
        """Convencience method to create a new httpx.AsyncClient.

        To be removed in favor of using the entire `AirbyteClient` class
        as a context manager.
        """
        warn(
            "Use of this method will be removed in a future release - "
            "please use the `AirbyteClient` class as a context manager.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._client

    async def __aenter__(self):
        """Context manager entry point."""
        if self._closed:
            raise RuntimeError(
                "The client cannot be started again after it has been closed."
            )
        if self._started:
            raise RuntimeError("The client cannot be started more than once.")

        self._started = True

        await self.check_health_status(self._client)

        return self

    async def __aexit__(self, *exc):
        """Context manager exit point."""

        self._closed = True
        await self._client.__aexit__()
