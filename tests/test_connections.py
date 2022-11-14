import pytest
from prefect.logging import disable_run_logger

from prefect_airbyte import exceptions as err
from prefect_airbyte.connections import trigger_sync

CONNECTION_ID = "e1b2078f-882a-4f50-9942-cfe34b2d825b"


async def example_trigger_sync_flow(airbyte_server):
    with disable_run_logger():
        return await trigger_sync.fn(
            airbyte_server=airbyte_server, connection_id=CONNECTION_ID
        )


async def test_successful_trigger_sync(
    mock_successful_connection_sync_calls, airbyte_server
):
    trigger_sync_result = await example_trigger_sync_flow(airbyte_server)

    assert type(trigger_sync_result) is dict

    assert trigger_sync_result == {
        "connection_id": "e1b2078f-882a-4f50-9942-cfe34b2d825b",
        "status": "active",
        "job_status": "succeeded",
        "job_created_at": 1650644844,
        "job_updated_at": 1650644844,
    }


async def test_cancelled_trigger_manual_sync(
    mock_cancelled_connection_sync_calls, airbyte_server
):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await example_trigger_sync_flow(airbyte_server)


async def test_connection_sync_inactive(mock_inactive_sync_calls, airbyte_server):
    with pytest.raises(err.AirbyteConnectionInactiveException):
        await example_trigger_sync_flow(airbyte_server)


async def test_failed_trigger_sync(mock_failed_connection_sync_calls, airbyte_server):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await example_trigger_sync_flow(airbyte_server)


async def test_bad_connection_id(mock_bad_connection_id_calls, airbyte_server):
    with pytest.raises(err.ConnectionNotFoundException):
        await example_trigger_sync_flow(airbyte_server)


async def test_failed_health_check(mock_failed_health_check_calls, airbyte_server):
    with pytest.raises(err.AirbyteServerNotHealthyException):
        await example_trigger_sync_flow(airbyte_server)


async def test_get_job_status_not_found(mock_invalid_job_status_calls, airbyte_server):
    with pytest.raises(err.JobNotFoundException):
        await example_trigger_sync_flow(airbyte_server)
