# prefect-airbyte

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-airbyte/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-airbyte?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-airbyte/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-airbyte?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-airbyte/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-airbyte?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-airbyte/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-airbyte?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Welcome!

`prefect-airbyte` is a collection of prebuilt Prefect tasks that can be used to quickly construct Prefect flows to trigger Airbyte syncs or export your connector configurations.

## Getting Started

### Python setup

Requires an installation of Python 3.7+

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Airbyte setup
See [the airbyte documention](https://docs.airbyte.com/deploying-airbyte) on how to get your own instance.

### Installation

Install `prefect-airbyte`

```bash
pip install prefect-airbyte
```

### Examples
#### Create an `AirbyteServer` block and save it
```python
from prefect_airbyte.server import AirbyteServer

# running airbyte locally at http://localhost:8000 with default auth
local_airbyte_server = AirbyteServer()

# running airbyte remotely at http://<someIP>:<somePort> as user `Marvin`
remote_airbyte_server = AirbyteServer(
    username="Marvin",
    password="DontPanic42",
    server_host="42.42.42.42",
    server_port="4242"
)

local_airbyte_server.save("my-local-airbyte-server")

remote_airbyte_server.save("my-remote-airbyte-server")

```


#### Trigger a defined connection sync
```python
from prefect import flow
from prefect_airbyte.connections import trigger_sync
from prefect_airbyte.server import AirbyteServer

@flow
def example_trigger_sync_flow():

      # Run other tasks and subflows here

      trigger_sync(
            airbyte_server=AirbyteServer.load("my-airbyte-server"),
            connection_id="your-connection-id-to-sync",
            poll_interval_s=3,
            status_updates=True
      )

example_trigger_sync_flow()
```

```console

❯ python airbyte_syncs.py
03:46:03 | prefect.engine - Created flow run 'thick-seahorse' for flow 'example_trigger_sync_flow'
03:46:03 | Flow run 'thick-seahorse' - Using task runner 'ConcurrentTaskRunner'
03:46:03 | Flow run 'thick-seahorse' - Created task run 'trigger_sync-35f0e9c2-0' for task 'trigger_sync'
03:46:03 | prefect - trigger airbyte connection: e1b2078f-882a-4f50-9942-cfe34b2d825b, poll interval 3 seconds
03:46:03 | prefect - pending
03:46:06 | prefect - running
03:46:09 | prefect - running
03:46:12 | prefect - running
03:46:16 | prefect - running
03:46:19 | prefect - running
03:46:22 | prefect - Job 26 succeeded.
03:46:22 | Task run 'trigger_sync-35f0e9c2-0' - Finished in state Completed(None)
03:46:22 | Flow run 'thick-seahorse' - Finished in state Completed('All states completed.')
```


#### Export an Airbyte instance's configuration

**NOTE**: The API endpoint corresponding to this task is no longer supported by open-source Airbyte versions as of v0.40.7. Check out the [Octavia CLI docs](https://github.com/airbytehq/airbyte/tree/master/octavia-cli) for more info.

```python
import gzip

from prefect import flow, task
from prefect_airbyte.configuration import export_configuration
from prefect_airbyte.server import AirbyteServer

@task
def zip_and_write_somewhere(
      airbyte_config: bytearray,
      somewhere: str,
):
    with gzip.open(somewhere, 'wb') as f:
        f.write(airbyte_config)

@flow
def example_export_configuration_flow(filepath: str):

    # Run other tasks and subflows here

    airbyte_config = export_configuration(
        airbyte_server=AirbyteServer.load("my-airbyte-server-block")
    )

    zip_and_write_somewhere(
        somewhere=filepath,
        airbyte_config=airbyte_config
    )

if __name__ == "__main__":
    example_export_configuration_flow('*://**/my_destination.gz')
```

## Resources

If you encounter and bugs while using `prefect-airbyte`, feel free to open an issue in the [prefect-airbyte](https://github.com/PrefectHQ/prefect-airbyte) repository.

If you have any questions or issues while using `prefect-airbyte`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack)

Feel free to ⭐️ or watch [`prefect-airbyte`](https://github.com/PrefectHQ/prefect-airbyte) for updates too!

## Development

If you'd like to install a version of `prefect-airbyte` for development, first clone the repository and then perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-airbyte.git

cd prefect-airbyte/

pip install -e ".[dev]"
```
