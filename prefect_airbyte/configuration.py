"""Tasks for updating and fetching Airbyte configurations"""
from prefect import get_run_logger, task

from prefect_airbyte.exceptions import AirbyteExportConfigurationFailed
from prefect_airbyte.server import AirbyteServer


@task
async def export_configuration(
    airbyte_server: AirbyteServer,
    timeout: int = 5,
) -> bytes:

    """
    Prefect Task that exports an Airbyte configuration via
    `{airbyte_server_host}/api/v1/deployment/export`.

    Args:
        airbyte_server: An `AirbyteServer` block for generating an `AirbyteClient`.
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

    airbyte = airbyte_server.get_client(logger=logger, timeout=timeout)

    logger.info("Initiating export of Airbyte configuration")
    try:
        return await airbyte.export_configuration()

    except AirbyteExportConfigurationFailed as e:
        logger.warning(
            "As of Airbyte v0.40.7-alpha, the Airbyte API no longer supports "
            "exporting configurations. See the Octavia CLI docs for more info."
        )
        raise e
