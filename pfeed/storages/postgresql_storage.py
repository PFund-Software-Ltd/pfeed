import os

from pfeed.storages.database_storage import DatabaseStorage
from pfeed.enums import IOFormat


# TODO
class PostgreSQLStorage(DatabaseStorage):
    SCHEME: str = "postgresql"
    SUPPORTED_IO_FORMATS = [IOFormat.POSTGRESQL]

    def __init__(self, storage_options: dict | None = None, io_options: dict | None = None):
        super().__init__(storage_options=storage_options, io_options=io_options)
        self.user = os.getenv("POSTGRES_USER", "pfunder")
        self.password = os.getenv("POSTGRES_PASSWORD", "password")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
    
    # e.g. postgresql://pfunder:password@localhost:5432
    def _create_uri(self) -> str:
        return f"{self.SCHEME}://{self.user}:{self.password}@{self.host}:{self.port}"