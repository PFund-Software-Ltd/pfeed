from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from sqlite3 import Connection as SQLiteConnection

    from pfeed._io.sqlite_io import SQLiteIO

from pfeed.enums import IOFormat
from pfeed.storages.file_backed_database_storage import FileBackedDatabaseStorage


class SQLiteStorage(FileBackedDatabaseStorage):
    SUPPORTED_IO_FORMATS: ClassVar[list[Literal[IOFormat.SQLITE]]] = [IOFormat.SQLITE]

    _io: SQLiteIO
    conn: SQLiteConnection | None

    def _create_uri(self) -> str:
        return ""

    def get_file_path(self) -> str:
        db_path = self._get_db_path()
        return db_path.db_uri

    def show_tables(self, include_schema: bool = True) -> list[str]:
        db_path = self._get_db_path()
        _ = self.io  # ensure the lazily-created IO is initialized
        return self._io.list_tables(db_path, include_schema=include_schema)
