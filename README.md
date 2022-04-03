# prefect-airbyte

## Welcome!
<!--  &emsp; <img src="imgs/airbyte.png" width="40" height="55" /> -->

`prefect-airbyte` is a collections of prebuilt Prefect tasks that can be used to quickly construct Prefect flows.

## Getting Started

### Python setup

Requires an installation of Python 3.7+

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-airbyte`

```bash
pip install prefect-airbyte
```

### Airbyte setup
See [the airbyte documention](https://docs.airbyte.com/deploying-airbyte) on how to get your own instance.


### Examples

#### Trigger a defined connection sync
```python
from prefect import flow
from prefect_airbyte.connections import trigger_sync


@flow
def example_trigger_sync_flow():

      # Run other tasks and subflows here

      trigger_sync(
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
```python
import gzip

from prefect import flow, task
from prefect_airbyte.configuration import export_configuration

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
        airbyte_server_host="localhost",
        airbyte_server_port="8000",
        airbyte_api_version="v1",
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

## Development

If you'd like to install a version of `prefect-airbyte` for development, first clone the repository and then perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-airbyte.git

cd prefect-airbyte/

pip install -e ".[dev]"
```
