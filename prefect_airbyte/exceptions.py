"""Exceptions to raise indicating issues throughout prefect_airbyte"""


class ConnectionNotFoundException(Exception):
    """
    Raises when a requested Airbyte connection cannot be found.
    """

    pass


class AirbyteServerNotHealthyException(Exception):
    """
    Raises when a specified Airbyte instance returns an unhealthy response.
    """

    pass


class JobNotFoundException(Exception):
    """
    Raises when a requested Airbyte job cannot be found.
    """

    pass


class AirbyteSyncJobFailed(Exception):
    """
    Raises when a specified Airbyte Sync Job fails.
    """

    pass


class AirbyteExportConfigurationFailed(Exception):
    """
    Raises when an Airbyte configuration export fails.
    """

    pass


class AirbyteConnectionInactiveException(Exception):
    """
    Raises when a specified Airbyte connection is inactive.
    """

    pass


class AirbyeConnectionDeprecatedException(Exception):
    """
    Raises when a specified Airbyte connection is deprecated.
    """

    pass
