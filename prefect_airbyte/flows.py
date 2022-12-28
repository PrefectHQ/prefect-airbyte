"""Flows for interacting with Airbyte."""

from prefect import flow, task

from prefect_airbyte.connections import AirbyteConnection, AirbyteSyncResult


@flow
async def run_connection_sync(
    airbyte_connection: AirbyteConnection,
) -> AirbyteSyncResult:
    """A flow that triggers a sync of an Airbyte connection and waits for it to complete.

    Args:
        airbyte_connection: `AirbyteConnection` representing the Airbyte connection to
            trigger and wait for completion of.

    Returns:
        `AirbyteSyncResult`: Model containing metadata for the `AirbyteSync`.

    Example:
        Define a flow that runs an Airbyte connection sync:
        ```python
        from prefect import flow
        from prefect_airbyte.server import AirbyteServer
        from prefect_airbyte.connections import AirbyteConnection
        from prefect_airbyte.flows import run_connection_sync

        airbyte_server = AirbyteServer(
            server_host="localhost",
            server_port=8000
        )

        connection = AirbyteConnection(
            airbyte_server=airbyte_server,
            connection_id="<YOUR-AIRBYTE-CONNECTION-UUID>"
        )

        @flow
        def airbyte_sync_flow():
            # do some things

            airbyte_sync_result = run_connection_sync(
                airbyte_connection=connection
            )
            print(airbyte_sync_result.records_synced)

            # do some other things, like trigger DBT based on number of new raw records
        ```
    """

    # TODO: refactor block method calls to avoid using <sync_compatible_method>.aio
    # we currently need to do this because of the deadlock caused by calling
    # a sync task within an async flow
    # see [this issue](https://github.com/PrefectHQ/prefect/issues/7551)

    airbyte_sync = await task(airbyte_connection.trigger.aio)(airbyte_connection)

    await task(airbyte_sync.wait_for_completion.aio)(airbyte_sync)

    return await task(airbyte_sync.fetch_result.aio)(airbyte_sync)
