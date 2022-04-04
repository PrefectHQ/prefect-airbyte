from prefect import flow, task

from prefect_airbyte.connections import trigger_sync


@flow
def my_test_flow():
    connection_id = "askjdfhdsjlkfhaksdjf"

    assert type(trigger_sync) is task

    assert type(trigger_sync(connection_id=connection_id).result()) is dict


def test_trigger_sync():
    my_test_flow()
