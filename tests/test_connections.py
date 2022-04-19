from prefect import flow

from prefect_airbyte.connections import trigger_sync


async def test_trigger_sync(airbyte_trigger_sync_response):
    @flow
    async def my_test_flow() -> dict:
        connection_id = "e1b2078f-882a-4f50-9942-cfe34b2d825b"

        return await trigger_sync(connection_id=connection_id)

    sync_info = await my_test_flow()

    trigger_sync_result = sync_info.result().result()

    assert type(trigger_sync_result) is dict

    assert all(k in airbyte_trigger_sync_response for k in trigger_sync_result)
