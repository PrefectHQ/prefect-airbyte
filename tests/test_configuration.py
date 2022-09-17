import pytest
from prefect import flow

from prefect_airbyte import exceptions as err
from prefect_airbyte.configuration import export_configuration


async def test_export_configuration(mock_successful_config_export_calls):
    @flow
    async def test_flow():
        return await export_configuration()

    export = await test_flow()
    assert type(export) is bytes


async def test_export_configuration_failed_health(mock_failed_health_check_calls):
    @flow
    async def test_flow():
        await export_configuration()

    with pytest.raises(err.AirbyteServerNotHealthyException):
        await test_flow()
