"""Exceptions to raise indicating issues throughout prefect_airbyte"""


class ConnectionNotFoundException(Exception):
    """
    Raises when a requested Airbyte connection cannot be found.
    """


class AirbyteServerNotHealthyException(Exception):
    """
    Raises when a specified Airbyte instance returns an unhealthy response.
    """


class JobNotFoundException(Exception):
    """
    Raises when a requested Airbyte job cannot be found.
    """


class AirbyteSyncJobFailed(Exception):
    """
    Raises when a specified Airbyte Sync Job fails.
    """


class AirbyteExportConfigurationFailed(Exception):
    """
    Raises when an Airbyte configuration export fails.
    """


class AirbyteConnectionInactiveException(Exception):
    """
    Raises when a specified Airbyte connection is inactive.
    """


class AirbyeConnectionDeprecatedException(Exception):
    """
    Raises when a specified Airbyte connection is deprecated.
    """
