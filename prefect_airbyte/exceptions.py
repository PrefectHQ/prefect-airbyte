class ConnectionNotFoundException(Exception):
    pass


class AirbyteServerNotHealthyException(Exception):
    pass


class JobNotFoundException(Exception):
    pass


class AirbyteSyncJobFailed(Exception):
    pass


class AirbyteExportConfigurationFailed(Exception):
    pass