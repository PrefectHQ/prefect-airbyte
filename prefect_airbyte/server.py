"""A module for defining OSS Airbyte interactions with Prefect."""

from logging import Logger
from typing import Optional

from prefect.blocks.core import Block
from pydantic import Field, HttpUrl, IPvAnyAddress, SecretStr

from prefect_airbyte.client import AirbyteClient


class AirbyteServer(Block):
    """A block representing an Airbyte server for generating `AirbyteClient` instances.

    Attributes:
        username: Username for Airbyte API.
        password: Password for Airbyte API.
        server_host: Hostname for Airbyte API.
        server_port: Port for Airbyte API.
        api_version: Version of Airbyte API to use.
        ```

    Example:
        Create an `AirbyteServer` block for an Airbyte instance running on localhost:
        ```python
        from prefect import flow
        from prefect_airbyte.connection import trigger_sync
        from prefect_airbyte.server import AirbyteServer


        @flow
        def airbyte_orchestration_flow():
            airbyte_server = AirbyteServer()
            trigger_sync(
                airbyte_server=airbyte_server,
                connection_id="my_connection_id",
            )
        ```
    """

    _block_type_name: Optional[str] = "Airbyte Server"
    _block_type_slug: Optional[str] = "airbyte-server"
    _logo_url: Optional[
        HttpUrl
    ] = "https://images.ctfassets.net/zscdif0zqppk/6gm7wsC7ANnKYQsm7oiSYz/aac1ad5e054d35d9e24af8d6ed3aed5f/59758427?h=250"  # noqa

    username: str = Field(
        default="airbyte",
        description="Username to authenticate with Airbyte API.",
    )

    password: SecretStr = Field(
        default=SecretStr("password"),
        description="Password to authenticate with Airbyte API.",
    )

    server_host: IPvAnyAddress = Field(
        default="127.0.0.1",
        description="Host address of Airbyte server.",
    )

    server_port: int = Field(
        default=8000,
        description="Port number of Airbyte server.",
    )

    api_version: str = Field(
        default="v1",
        description="Airbyte API version to use.",
    )

    @property
    def airbyte_base_url(self) -> str:
        return f"http://{self.server_host}:{self.server_port}/api/{self.api_version}"

    def get_client(self, logger: Logger, timeout: int = 10) -> AirbyteClient:
        """Returns an `AirbyteClient` instance for interacting with the Airbyte API.

        Args:
            logger: Logger instance used to log messages related to API calls.
            timeout: The number of seconds to wait before an API call times out.

        Returns:
            An `AirbyteClient` instance.
        """
        return AirbyteClient(
            logger=logger,
            airbyte_base_url=self.airbyte_base_url,
            auth=(self.username, self.password.get_secret_value()),
            timeout=timeout,
        )
