"""Tasks for updating and fetching Airbyte configurations"""

from prefect import task
from prefect.logging.loggers import get_logger

from prefect_airbyte.client import AirbyteClient

@task
async def export_configuration(
    airbyte_server_host: str = "localhost",
    airbyte_server_port: int = "8000",
    airbyte_api_version: str = "v1",
) -> bytearray:

    """
    Task that triggers an export of an Airbyte configuration per `{AIRBYTE_HOST}/api/v1/deployment/export`

    Args:
        airbyte_server_host (str, optional): Hostname of Airbyte server where connection is
            configured. Will overwrite the value provided at init if provided.
        airbyte_server_port (str, optional): Port that the Airbyte server is listening on.
            Will overwrite the value provided at init if provided.
        airbyte_api_version (str, optional): Version of Airbyte API to use to trigger connection
            sync. Will overwrite the value provided at init if provided.

    Returns:
        bytearray: byte array containing Airbyte configuration
    
    Examples:
    
        Flow that writes the Airbyte configuration as a gzip to a filepath:
    
        ```python
        import gzip

        from prefect import flow, task
        from prefect_airbyte.configuration import export_configuration

        @task
        def zip_and_write_somewhere(
            airbyte_config: bytearray
            somwhere: str = 'my_destination.gz','
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
    
    logger = get_logger()

    airbyte_base_url = (
        f"http://{airbyte_server_host}:"
        f"{airbyte_server_port}/api/{airbyte_api_version}"
    )

    airbyte = AirbyteClient(logger, airbyte_base_url)
    session = airbyte.establish_session()

    logger.info("Initiating export of Airbyte configuration")
    airbyte_config = airbyte.export_configuration(
        airbyte_base_url=airbyte_base_url,
        session=session
    )

    return airbyte_config