import pytest
from prefect.logging import disable_run_logger

from prefect_airbyte import exceptions as err
from prefect_airbyte.configuration import export_configuration
from prefect_airbyte.exceptions import AirbyteExportConfigurationFailed


async def test_export_configuration(
    mock_successful_config_export_calls, airbyte_server
):
    with disable_run_logger():
        export = await export_configuration.fn(airbyte_server=airbyte_server)

        assert type(export) is bytes


async def test_export_configuration_using_kwargs(
    mock_successful_config_export_calls, airbyte_server
):
    with disable_run_logger():
        export = await export_configuration.fn(
            airbyte_server_host="localhost",
            airbyte_server_port=8000,
        )

        assert type(export) is bytes


async def test_export_configuration_failed_health(
    mock_failed_health_check_calls, airbyte_server
):
    with pytest.raises(err.AirbyteServerNotHealthyException):
        with disable_run_logger():
            await export_configuration.fn(airbyte_server=airbyte_server)


async def test_export_configuration_raise_deprecation_warning(
    mock_config_endpoint_not_found, airbyte_server
):
    with pytest.raises(AirbyteExportConfigurationFailed):
        with disable_run_logger():
            await export_configuration.fn(airbyte_server=airbyte_server)
