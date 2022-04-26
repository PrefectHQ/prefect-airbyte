from prefect_airbyte.configuration import export_configuration


async def test_successful_trigger_sync(mock_successful_config_export_calls):
    export = await export_configuration.fn()

    assert type(export) is bytes
