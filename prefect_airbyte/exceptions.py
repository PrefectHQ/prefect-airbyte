"""Exceptions to raise indicating issues throughout prefect_airbyte"""


class ConnectionNotFoundException(Exception):
    """
    custom exception class

    Args:
        Exception: base Exception class

    Returns:
        - ConnectionNotFoundException
    """

    pass


class AirbyteServerNotHealthyException(Exception):
    """
    custom exception class

    Args:
        Exception: base Exception class

    Returns:
        - AirbyteServerNotHealthyException
    """

    pass


class JobNotFoundException(Exception):
    """
    custom exception class

    Args:
        Exception: base Exception class

    Returns:
        - JobNotFoundException
    """

    pass


class AirbyteSyncJobFailed(Exception):
    """
    custom exception class

    Args:
        Exception: base Exception class

    Returns:
        - AirbyteSyncJobFailed
    """

    pass


class AirbyteExportConfigurationFailed(Exception):
    """
    custom exception class

    Args:
        Exception: base Exception class

    Returns:
        - AirbyteExportConfigurationFailed
    """

    pass
