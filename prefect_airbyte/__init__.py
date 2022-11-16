from . import _version

from prefect_airbyte.server import AirbyteServer  # noqa F401

__version__ = _version.get_versions()["version"]
