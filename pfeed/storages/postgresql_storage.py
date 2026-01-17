from pfeed.enums import DataStorage
from pfeed.storages.database_storage import DatabaseStorage


# TODO
class PostgreSQLStorage(DatabaseStorage):
    name = DataStorage.POSTGRESQL
    
    