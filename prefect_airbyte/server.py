"""A module for defining OSS Airbyte interactions with Prefect."""

from logging import Logger

from prefect.blocks.core import Block
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field, SecretStr
else:
    from pydantic import Field, SecretStr

from prefect_airbyte.client import AirbyteClient


class AirbyteServer(Block):
    """A block representing an Airbyte server for generating `AirbyteClient` instances.

    Attributes:
        username: Username for Airbyte API.
        password: Password for Airbyte API.
        server_host: Hostname for Airbyte API.
        server_port: Port for Airbyte API.
        api_version: Version of Airbyte API to use.
        use_ssl: Whether to use a secure url for calls to the Airbyte API.

    Example:
        ```python
        from prefect_airbyte.server import AirbyteServer

        server = AirbyteServer.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "Airbyte Server"
    _block_type_slug = "airbyte-server"
    _logo_url = "https://cdn.sanity.io/images/3ugk85nk/production/7f50097d1915fe75b0ee84c951c742a83d3c53cb-250x250.png"  # noqa
    _documentation_url = "https://prefecthq.github.io/prefect-airbyte/server/#prefect_airbyte.server.AirbyteServer"  # noqa

    username: str = Field(
        default="airbyte",
        description="Username to authenticate with Airbyte API.",
    )

    password: SecretStr = Field(
        default=SecretStr("password"),
        description="Password to authenticate with Airbyte API.",
    )

    server_host: str = Field(
        default="localhost",
        description="Host address of Airbyte server.",
        example="127.0.0.1",
    )

    server_port: int = Field(
        default=8000,
        description="Port number of Airbyte server.",
    )

    api_version: str = Field(
        default="v1",
        description="Airbyte API version to use.",
        title="API Version",
    )

    use_ssl: bool = Field(
        default=False,
        description="Whether to use SSL when connecting to Airbyte server.",
        title="Use SSL",
    )

    @property
    def base_url(self) -> str:
        """Property containing the base URL for the Airbyte API."""
        protocol = "https" if self.use_ssl else "http"
        return (
            f"{protocol}://{self.server_host}:{self.server_port}/api/{self.api_version}"
        )

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
            airbyte_base_url=self.base_url,
            auth=(self.username, self.password.get_secret_value()),
            timeout=timeout,
        )
