import pytest
from prefect import flow

from prefect_airbyte import exceptions as err
from prefect_airbyte.connections import trigger_sync

CONNECTION_ID = "e1b2078f-882a-4f50-9942-cfe34b2d825b"


@flow
async def basic_trigger_sync_flow(stream_logs=False):
    return await trigger_sync(connection_id=CONNECTION_ID, stream_logs=stream_logs)


async def test_successful_trigger_sync(mock_successful_connection_sync_calls):

    trigger_sync_result = await basic_trigger_sync_flow()

    assert trigger_sync_result == {
        "connection_id": "e1b2078f-882a-4f50-9942-cfe34b2d825b",
        "status": "active",
        "job_status": "succeeded",
        "job_created_at": 1650644844,
        "job_updated_at": 1650644844,
        "logs": None,
    }


async def test_successful_trigger_sync_with_logs(mock_successful_connection_sync_calls):

    trigger_sync_result = await basic_trigger_sync_flow(stream_logs=True)

    assert trigger_sync_result == {
        "connection_id": "e1b2078f-882a-4f50-9942-cfe34b2d825b",
        "status": "active",
        "job_status": "succeeded",
        "job_created_at": 1650644844,
        "job_updated_at": 1650644844,
        "logs": "loggy log logs".split(),
    }


async def test_cancelled_trigger_manual_sync(mock_cancelled_connection_sync_calls):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await basic_trigger_sync_flow()


async def test_connection_sync_inactive(mock_inactive_sync_calls):
    with pytest.raises(err.AirbyteConnectionInactiveException):
        await basic_trigger_sync_flow()


async def test_failed_trigger_sync(mock_failed_connection_sync_calls):
    with pytest.raises(err.AirbyteSyncJobFailed):
        await basic_trigger_sync_flow()


async def test_bad_connection_id(mock_bad_connection_id_calls):
    with pytest.raises(err.ConnectionNotFoundException):
        await basic_trigger_sync_flow()


async def test_failed_health_check(mock_failed_health_check_calls):
    with pytest.raises(err.AirbyteServerNotHealthyException):
        await basic_trigger_sync_flow()


async def test_get_job_status_not_found(mock_invalid_job_status_calls):
    with pytest.raises(err.JobNotFoundException):
        await basic_trigger_sync_flow()


async def test_get_job_status_no_logs(mock_successful_connection_sync_calls):
    job_metadata = await basic_trigger_sync_flow()

    assert not job_metadata["logs"]


async def test_get_job_status_stream_logs(mock_successful_connection_sync_calls):
    job_metadata = await basic_trigger_sync_flow(stream_logs=True)

    assert isinstance(job_metadata["logs"], list)
    assert isinstance(job_metadata["logs"][0], str)
