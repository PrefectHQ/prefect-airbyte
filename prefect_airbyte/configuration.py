"""Tasks for updating and fetching Airbyte configurations"""
from typing import Optional

from prefect import get_run_logger, task

from prefect_airbyte.server import AirbyteServer


@task
async def export_configuration(
    airbyte_server: Optional[AirbyteServer] = None,
    airbyte_server_host: Optional[str] = None,
    airbyte_server_port: Optional[int] = None,
    airbyte_api_version: Optional[str] = None,
    timeout: int = 5,
) -> bytes:

    """
    Prefect Task that exports an Airbyte configuration via
    `{airbyte_server_host}/api/v1/deployment/export`.

    As of `prefect-airbyte==0.2.0`, the kwargs `airbyte_server_host` and
    `airbyte_server_port` can be replaced by passing an `airbyte_server` block
    instance to generate the `AirbyteClient`. Using the `airbyte_server` block is
    preferred, but the individual kwargs remain for backwards compatibility.

    Args:
        airbyte_server: An `AirbyteServer` block for generating an `AirbyteClient`.
        airbyte_server_host: Airbyte server host to connect to.
        airbyte_server_port: Airbyte server port to connect to.
        airbyte_api_version: Airbyte API version to use.
        timeout: Timeout in seconds on the `httpx.AsyncClient`.

    Returns:
        Bytes containing Airbyte configuration

    Examples:

        Flow that writes the Airbyte configuration as a gzip to a filepath:

        ```python
        import gzip

        from prefect import flow, task
        from prefect_airbyte.configuration import export_configuration
        from prefect_airbyte.server import AirbyteServer

        @task
        def zip_and_write_somewhere(
            airbyte_configuration: bytes
            somewhere: str = 'my_destination.gz','
        ):
            with gzip.open('my_destination.gz', 'wb') as f:
                    f.write(airbyte_configuration)

        @flow
        def example_export_configuration_flow():

            # Run other tasks and subflows here

            airbyte_config = export_configuration(
                airbyte_server=AirbyteServer.load("oss-airbyte")
            )

            zip_and_write_somewhere(airbyte_config=airbyte_config)

        example_trigger_sync_flow()
        ```
    """
    logger = get_run_logger()

    if not airbyte_server:
        logger.warning(
            "Using kwargs `airbyte_server_host`, `airbyte_server_port`, `airbyte_api_version` "  # noqa
            "will be deprecated. Please pass an `airbyte_server` block to this task instead."  # noqa
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
            logger.warning(
                "Ignoring kwargs `airbyte_server_host` and `airbyte_server_port` "
                "because `airbyte_server` block was passed."
            )

    async with airbyte_server.get_client(
        logger=logger, timeout=timeout
    ) as airbyte_client:

        logger.info("Initiating export of Airbyte configuration")

        return await airbyte_client.export_configuration()
