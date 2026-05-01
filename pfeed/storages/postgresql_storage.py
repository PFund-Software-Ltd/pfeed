from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pathlib import Path
    
import os

from pfeed.storages.database_storage import DatabaseStorage
from pfeed.enums import IOFormat, DataLayer


# TODO
class PostgreSQLStorage(DatabaseStorage):
    SCHEME: str = "postgresql"
    SUPPORTED_IO_FORMATS = [IOFormat.POSTGRESQL]

    def __init__(
        self,
        data_path: str | None,
        storage_options: dict[str, Any] | None = None,
        **kwargs: Any,  # additional kwargs for compatibility with other storages
    ):
        '''
        Args:
            data_path: database uri, e.g. postgresql://pfunder:password@localhost:5432/postgres
        '''
        if data_path is None:
            self.user = os.getenv("POSTGRES_USER", "pfunder")
            self.password = os.getenv("POSTGRES_PASSWORD", "password")
            self.host = os.getenv("POSTGRES_HOST", "localhost")
            self.port = os.getenv("POSTGRES_PORT", "5432")
            self.database = os.getenv("POSTGRES_DATABASE", "postgres")
        else:
            from urllib.parse import urlparse, unquote
            parsed = urlparse(data_path)
            # TODO: check if any of the fields are None, if so, raise Error
            self.user = unquote(parsed.username) if parsed.username else None
            self.password = unquote(parsed.password) if parsed.password else None
            self.host = parsed.hostname  # already lowercased, IPv6-safe
            self.port = parsed.port      # already int, or None
            self.database = parsed.path.lstrip('/') or None
        super().__init__(data_path=data_path, storage_options=storage_options, **kwargs)
    
    # e.g. postgresql://pfunder:password@localhost:5432
    def _create_uri(self) -> str:
        return f"{self.SCHEME}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"