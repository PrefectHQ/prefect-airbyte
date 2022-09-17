"""Tasks for updating and fetching Airbyte configurations"""
from prefect import get_run_logger, task

from prefect_airbyte.client import AirbyteClient


@task
async def export_configuration(
    airbyte_server_host: str = "localhost",
    airbyte_server_port: int = "8000",
    airbyte_api_version: str = "v1",
    timeout: int = 5,
) -> bytes:

    """
    Prefect Task that exports an Airbyte configuration via
    `{airbyte_server_host}/api/v1/deployment/export`.

    Args:
        airbyte_server_host: Airbyte instance hostname where connection is configured.
        airbyte_server_port: Port where Airbyte instance is listening.
        airbyte_api_version: Version of Airbyte API to use to export configuration.
        timeout: Timeout in seconds on the `httpx.AsyncClient`.

    Returns:
        Bytes containing Airbyte configuration

    Examples:

        Flow that writes the Airbyte configuration as a gzip to a filepath:

        ```python
        import gzip

        from prefect import flow, task
        from prefect_airbyte.configuration import export_configuration

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
                    airbyte_server_host="localhost",
                    airbyte_server_port="8000",
                    airbyte_api_version="v1",
            )

            zip_and_write_somewhere(airbyte_config=airbyte_config)

        example_trigger_sync_flow()
        ```
    """

    logger = get_run_logger()

    airbyte_base_url = (
        f"http://{airbyte_server_host}:"
        f"{airbyte_server_port}/api/{airbyte_api_version}"
    )

    airbyte = AirbyteClient(logger, airbyte_base_url, timeout=timeout)

    logger.info("Initiating export of Airbyte configuration")
    airbyte_config = await airbyte.export_configuration()

    return airbyte_config
