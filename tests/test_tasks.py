from prefect import flow

from prefect_airbyte.tasks import (
    goodbye_prefect_airbyte,
    hello_prefect_airbyte,
)


def test_hello_prefect_airbyte():
    @flow
    def test_flow():
        return hello_prefect_airbyte()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-airbyte!"


def goodbye_hello_prefect_airbyte():
    @flow
    def test_flow():
        return goodbye_prefect_airbyte()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-airbyte!"
