import pytest
from prefect import flow

from prefect_airbyte.connections import AirbyteSyncResult
from prefect_airbyte.exceptions import AirbyteSyncJobFailed
from prefect_airbyte.flows import run_connection_sync

expected_airbyte_sync_result = AirbyteSyncResult(
    created_at=1650644844,
    job_status="succeeded",
    job_id=45,
    records_synced=0,
    updated_at=1650644844,
)


async def test_run_connection_sync_standalone_success(
    airbyte_server, airbyte_connection, mock_successful_connection_sync_calls
):

    result = await run_connection_sync(airbyte_connection=airbyte_connection)

    assert result == expected_airbyte_sync_result


async def test_run_connection_sync_standalone_fail(
    airbyte_server, airbyte_connection, mock_failed_connection_sync_calls, caplog
):

    with pytest.raises(AirbyteSyncJobFailed):
        await run_connection_sync(airbyte_connection=airbyte_connection)


async def test_run_connection_sync_standalone_cancel(
    airbyte_server, airbyte_connection, mock_cancelled_connection_sync_calls
):

    with pytest.raises(AirbyteSyncJobFailed):
        await run_connection_sync(airbyte_connection=airbyte_connection)


async def test_run_connection_sync_standalone_status_updates(
    airbyte_server, airbyte_connection, mock_successful_connection_sync_calls, caplog
):
    airbyte_connection.status_updates = True
    await run_connection_sync(airbyte_connection=airbyte_connection)

    assert "Job 45 succeeded" in caplog.text


async def test_run_connection_sync_subflow_synchronously(
    airbyte_server, airbyte_connection, mock_successful_connection_sync_calls
):
    @flow
    def airbyte_sync_sync_flow():
        return run_connection_sync(airbyte_connection=airbyte_connection)

    result = airbyte_sync_sync_flow()

    assert result == expected_airbyte_sync_result


async def test_run_connection_sync_subflow_asynchronously(
    airbyte_server, airbyte_connection, mock_successful_connection_sync_calls
):
    @flow
    async def airbyte_sync_sync_flow():
        return await run_connection_sync(airbyte_connection=airbyte_connection)

    result = await airbyte_sync_sync_flow()

    assert result == expected_airbyte_sync_result
