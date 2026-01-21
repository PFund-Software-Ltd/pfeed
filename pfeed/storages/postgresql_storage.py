from pfeed.storages.database_storage import DatabaseStorage
from pfeed.enums import IOFormat


# TODO
class PostgreSQLStorage(DatabaseStorage):
    SUPPORTED_IO_FORMATS = [IOFormat.POSTGRESQL]
