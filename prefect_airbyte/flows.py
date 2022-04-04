"""This is an example flows module"""
from prefect import flow

from prefect_airbyte.tasks import (
    goodbye_prefect_airbyte,
    hello_prefect_airbyte,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_airbyte)
    print(goodbye_prefect_airbyte)
