import pytest
import respx
from httpx import Response

from prefect_airbyte.server import AirbyteServer

# connection fixtures / mocks

CONNECTION_ID = "e1b2078f-882a-4f50-9942-cfe34b2d825b"


@pytest.fixture
def airbyte_server():
    return AirbyteServer()


@pytest.fixture
def airbyte_trigger_sync_response() -> dict:
    return {
        "connectionId": CONNECTION_ID,
        "status": "active",
        "job": {"id": 45, "createdAt": 1650311569, "updatedAt": 1650311585},
    }


@pytest.fixture
def airbyte_good_health_check_response() -> dict:
    return {"available": True}


@pytest.fixture
def airbyte_bad_health_check_response() -> dict:
    return {"available": False}


@pytest.fixture
def airbyte_get_connection_response_json() -> dict:
    return {
        "connectionId": CONNECTION_ID,
        "name": "File <> Snowflake Demo",
        "namespaceDefinition": "destination",
        "namespaceFormat": "${SOURCE_NAMESPACE}",
        "prefix": "",
        "sourceId": "6054b03b-a9bd-4090-96a7-08076f066975",
        "destinationId": "c5ad053e-5741-4774-a60c-f976fe95c4dd",
        "operationIds": [],
        "syncCatalog": {
            "streams": [
                {
                    "stream": {
                        "name": "epidemic_stats",
                        "jsonSchema": {
                            "type": "object",
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "properties": {
                                "key": {"type": ["string", "null"]},
                                "date": {"type": ["string", "null"]},
                                "new_tested": {"type": ["number", "null"]},
                                "new_deceased": {"type": ["number", "null"]},
                                "total_tested": {"type": ["number", "null"]},
                                "new_confirmed": {"type": ["number", "null"]},
                                "new_recovered": {"type": ["number", "null"]},
                                "total_deceased": {"type": ["number", "null"]},
                                "total_confirmed": {"type": ["number", "null"]},
                                "total_recovered": {"type": ["number", "null"]},
                            },
                        },
                        "supportedSyncModes": ["full_refresh"],
                        "sourceDefinedCursor": None,
                        "defaultCursorField": [],
                        "sourceDefinedPrimaryKey": [],
                        "namespace": None,
                    },
                    "config": {
                        "syncMode": "full_refresh",
                        "cursorField": [],
                        "destinationSyncMode": "overwrite",
                        "primaryKey": [],
                        "aliasName": "epidemic_stats",
                        "selected": True,
                    },
                }
            ]
        },
        "schedule": None,
        "status": "active",
        "resourceRequirements": None,
    }


@pytest.fixture
def airbyte_get_inactive_connection_response(
    airbyte_get_connection_response_json,
) -> dict:
    airbyte_get_connection_response_json["status"] = "inactive"
    return airbyte_get_connection_response_json


@pytest.fixture
def airbyte_get_connection_not_found():
    return {
        "id": "string",
        "message": "string",
        "exceptionClassName": "string",
        "exceptionStack": ["string"],
        "rootCauseExceptionClassName": "string",
        "rootCauseExceptionStack": ["string"],
    }


@pytest.fixture
def airbyte_base_job_status_response() -> dict:
    return {
        "job": {
            "id": 45,
            "configType": "sync",
            "configId": CONNECTION_ID,
            "createdAt": 1650644844,
            "updatedAt": 1650644844,
            "status": None,
        },
        "attempts": [],
    }


@pytest.fixture
def airbyte_get_good_job_status_response(airbyte_base_job_status_response) -> dict:
    airbyte_base_job_status_response["job"]["status"] = "succeeded"
    return airbyte_base_job_status_response


@pytest.fixture
def airbyte_get_pending_job_status_response(airbyte_base_job_status_response) -> dict:
    airbyte_base_job_status_response["job"]["status"] = "pending"
    return airbyte_base_job_status_response


@pytest.fixture
def airbyte_get_failed_job_status_response(airbyte_base_job_status_response) -> dict:
    airbyte_base_job_status_response["job"]["status"] = "failed"
    return airbyte_base_job_status_response


@pytest.fixture
def airbyte_job_status_not_found_response():
    return {
        "id": "string",
        "message": "string",
        "exceptionClassName": "string",
        "exceptionStack": ["string"],
        "rootCauseExceptionClassName": "string",
        "rootCauseExceptionStack": ["string"],
    }


@pytest.fixture
def base_airbyte_url():
    return "http://localhost:8000/api/v1"


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_successful_connection_sync_calls(
    respx_mock,
    base_airbyte_url,
    airbyte_good_health_check_response,
    airbyte_get_connection_response_json,
    airbyte_trigger_sync_response,
    airbyte_get_good_job_status_response,
):
    # health check: successful case
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    # get existing connection by ID: successful case
    respx_mock.post(
        url=f"{base_airbyte_url}/connections/get/",
        json={"connectionId": airbyte_get_connection_response_json["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_get_connection_response_json))

    # trigger sync of existing connection: successful case
    respx_mock.post(
        url=f"{base_airbyte_url}/connections/sync/",
        json={"connectionId": airbyte_trigger_sync_response["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_trigger_sync_response))

    # get job status: successful case
    respx_mock.post(
        url=f"{base_airbyte_url}/jobs/get/",
        json={"id": airbyte_get_good_job_status_response["job"]["id"]},
    ).mock(return_value=Response(200, json=airbyte_get_good_job_status_response))


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_failed_connection_sync_calls(
    respx_mock,
    base_airbyte_url,
    airbyte_good_health_check_response,
    airbyte_get_connection_response_json,
    airbyte_trigger_sync_response,
    airbyte_get_failed_job_status_response,
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/get/",
        json={"connectionId": airbyte_get_connection_response_json["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_get_connection_response_json))

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/sync/",
        json={"connectionId": airbyte_trigger_sync_response["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_trigger_sync_response))

    respx_mock.post(
        url=f"{base_airbyte_url}/jobs/get/",
        json={"id": airbyte_get_failed_job_status_response["job"]["id"]},
    ).mock(return_value=Response(200, json=airbyte_get_failed_job_status_response))


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_failed_health_check_calls(
    respx_mock, base_airbyte_url, airbyte_bad_health_check_response
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_bad_health_check_response)
    )


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_bad_connection_id_calls(
    respx_mock,
    base_airbyte_url,
    airbyte_get_connection_response_json,
    airbyte_get_connection_not_found,
    airbyte_good_health_check_response,
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/get/",
        json={"connectionId": airbyte_get_connection_response_json["connectionId"]},
    ).mock(return_value=Response(404, json=airbyte_get_connection_not_found))


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_invalid_job_status_calls(
    respx_mock,
    base_airbyte_url,
    airbyte_good_health_check_response,
    airbyte_get_good_job_status_response,
    airbyte_trigger_sync_response,
    airbyte_get_connection_response_json,
    airbyte_job_status_not_found_response,
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/get/",
        json={"connectionId": airbyte_get_connection_response_json["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_get_connection_response_json))

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/sync/",
        json={"connectionId": airbyte_trigger_sync_response["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_trigger_sync_response))

    respx_mock.post(
        url=f"{base_airbyte_url}/jobs/get/",
        json={"id": airbyte_get_good_job_status_response["job"]["id"]},
    ).mock(return_value=Response(404, json=airbyte_job_status_not_found_response))


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_cancelled_connection_sync_calls(
    respx_mock,
    base_airbyte_url,
    airbyte_good_health_check_response,
    airbyte_get_good_job_status_response,
    airbyte_trigger_sync_response,
    airbyte_get_connection_response_json,
    airbyte_get_pending_job_status_response,
    airbyte_get_failed_job_status_response,
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/get/",
        json={"connectionId": airbyte_get_connection_response_json["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_get_connection_response_json))

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/sync/",
        json={"connectionId": airbyte_trigger_sync_response["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_trigger_sync_response))

    respx_mock.post(
        url=f"{base_airbyte_url}/jobs/get/",
        json={"id": airbyte_get_good_job_status_response["job"]["id"]},
    ).mock(return_value=Response(200, json=airbyte_get_pending_job_status_response))

    respx_mock.post(
        url=f"{base_airbyte_url}/jobs/get/",
        json={"id": airbyte_get_good_job_status_response["job"]["id"]},
    ).mock(return_value=Response(200, json=airbyte_get_failed_job_status_response))


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_inactive_sync_calls(
    respx_mock,
    base_airbyte_url,
    airbyte_good_health_check_response,
    airbyte_get_connection_response_json,
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    airbyte_get_connection_response_json["status"] = "inactive"

    respx_mock.post(
        url=f"{base_airbyte_url}/connections/get/",
        json={"connectionId": airbyte_get_connection_response_json["connectionId"]},
    ).mock(return_value=Response(200, json=airbyte_get_connection_response_json))


# configuration fixtures / mocks


@pytest.fixture
def airbyte_good_export_configuration_response() -> bytes:
    return b""


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_successful_config_export_calls(
    respx_mock,
    base_airbyte_url,
    airbyte_good_health_check_response,
    airbyte_good_export_configuration_response,
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    respx_mock.post(url=f"{base_airbyte_url}/deployment/export/").mock(
        return_value=Response(200, content=airbyte_good_export_configuration_response)
    )


@respx.mock(assert_all_called=True)
@pytest.fixture
def mock_config_endpoint_not_found(
    respx_mock,
    base_airbyte_url,
    airbyte_good_health_check_response,
    airbyte_good_export_configuration_response,
):
    respx_mock.get(url=f"{base_airbyte_url}/health/").mock(
        return_value=Response(200, json=airbyte_good_health_check_response)
    )

    respx_mock.post(url=f"{base_airbyte_url}/deployment/export/").mock(
        return_value=Response(404)
    )
