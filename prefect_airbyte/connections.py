"""Tasks for controlling airbyte connections"""
from typing import TYPE_CHECKING, Dict, Optional, Sequence, Union

from prefect import get_run_logger, task


@task
async def trigger_sync(
    connection_id: str
) -> Dict:
    """
    Triggers a manual sync of a defined Airbyte connection

    Args:
        connection_id: The ID of the connection defined in Airbyte (found in URL)

    Returns:
        Dict: TODO

    Examples:
        TODO

        ```python
        from prefect import flow
        from prefect.context import get_run_context


        @flow
        def trigger_airbyte_sync():
            pass
        ```
    """  # noqa
    pass