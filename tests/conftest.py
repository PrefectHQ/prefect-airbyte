import pytest


@pytest.fixture
def airbyte_trigger_sync_response():
    return {
        "connection_id": "e1b2078f-882a-4f50-9942-cfe34b2d825b",
        "status": "active",
        "job_status": "succeeded",
        "job_created_at": 1650311569,
        "job_updated_at": 1650311585,
    }
