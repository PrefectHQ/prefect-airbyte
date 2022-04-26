import pytest

from prefect_airbyte import exceptions as err
from prefect_airbyte.connections import trigger_sync

CONNECTION_ID = "e1b2078f-882a-4f50-9942-cfe34b2d825b"


async def test_successful_trigger_sync(mock_successful_connection_sync_calls):
    trigger_sync_result = await trigger_sync.fn(connection_id=CONNECTION_ID)

    assert type(trigger_sync_result) is dict

    assert trigger_sync_result == {
        "connection_id": "e1b2078f-882a-4f50-9942-cfe34b2d825b",
        "status": "active",
        "job_status": "succeeded",
        "job_created_at": 1650644844,
        "job_updated_at": 1650644844,
    }


async def test_canceled_trigger_manual_sync(mock_cancelled_connection_sync_calls):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await trigger_sync.fn(connection_id=CONNECTION_ID)


async def test_failed_trigger_sync(mock_failed_connection_sync_calls):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await trigger_sync.fn(connection_id=CONNECTION_ID)


async def test_bad_connection_id(mock_bad_connection_id_calls):
    with pytest.raises(err.ConnectionNotFoundException):
        await trigger_sync.fn(connection_id=CONNECTION_ID)


async def test_failed_health_check(mock_failed_health_check_calls):
    with pytest.raises(err.AirbyteServerNotHealthyException):
        await trigger_sync.fn(connection_id=CONNECTION_ID)


async def test_get_job_status_not_found(mock_invalid_job_status_calls):
    with pytest.raises(err.JobNotFoundException):
        await trigger_sync.fn(connection_id=CONNECTION_ID)
