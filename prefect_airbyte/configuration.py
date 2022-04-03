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
    """
    
    logger = get_logger()

    airbyte_base_url = (
        f"http://{airbyte_server_host}:"
        f"{airbyte_server_port}/api/{airbyte_api_version}"
    )

    airbyte = AirbyteClient(logger, airbyte_base_url)
    session = airbyte.establish_session()

    logger.info("Initiating export of Airbyte configuration")
    airbyte_config = airbyte.export_configuration(session)

    return airbyte_config