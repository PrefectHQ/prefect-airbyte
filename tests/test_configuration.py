import pytest

from prefect_airbyte import exceptions as err
from prefect_airbyte.configuration import export_configuration


async def test_export_configuration(mock_successful_config_export_calls):
    export = await export_configuration.fn()

    assert type(export) is bytes


async def test_export_configuration_failed_health(mock_failed_health_check_calls):
    with pytest.raises(err.AirbyteServerNotHealthyException):
        await export_configuration.fn()
