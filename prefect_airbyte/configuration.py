from connections import AirbyteClient

from prefect import task
from prefect.logging.loggers import get_logger

@task
async def configuration_export(
        airbyte_server_host: str = "localhost",
        airbyte_server_port: int = None,
        airbyte_api_version: str = None,
    ) -> bytearray:
    
        """
        Task run method triggering an export of an Airbyte configuration

        Args:
            - airbyte_server_host (str, optional): Hostname of Airbyte server where connection is
                configured. Will overwrite the value provided at init if provided.
            - airbyte_server_port (str, optional): Port that the Airbyte server is listening on.
                Will overwrite the value provided at init if provided.
            - airbyte_api_version (str, optional): Version of Airbyte API to use to trigger connection
                sync. Will overwrite the value provided at init if provided.

        Returns:
            - byte array of Airbyte configuration data
        """
        
        logger = get_logger()

        airbyte_base_url = (
            f"http://{airbyte_server_host}:"
            f"{airbyte_server_port}/api/{airbyte_api_version}"
        )

        airbyte = AirbyteClient(get_logger(), airbyte_base_url)
        session = airbyte._establish_session()

        logger.info("Initiating export of Airbyte configuration")
        airbyte_config = airbyte._export_configuration(session)

        return airbyte_config