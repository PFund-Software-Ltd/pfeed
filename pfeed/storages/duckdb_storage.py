from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from pfeed._io.duckdb_io import DuckDBIO

from pfeed.enums import IOFormat
from pfeed.storages.file_backed_database_storage import FileBackedDatabaseStorage


class DuckDBStorage(FileBackedDatabaseStorage):
    SUPPORTED_IO_FORMATS: ClassVar[list[Literal[IOFormat.DUCKDB]]] = [IOFormat.DUCKDB]

    _io: DuckDBIO
    conn: DuckDBPyConnection | None

    def _create_uri(self) -> str:
        return ''

    def get_file_path(self) -> str:
        db_path = self._get_db_path()
        return db_path.db_uri

    def show_tables(self, include_schema: bool = True) -> list[str]:
        if self.conn is None:
            raise ValueError('DuckDB connection is not established')
        db_path = self._get_db_path()
        schema_name = db_path.schema_name
        if include_schema:
            result = self.conn.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = ?
            """, [schema_name]).fetchall()
            return [f"{row[0]}.{row[1]}" for row in result]
        else:
            result = self.conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ?
            """, [schema_name]).fetchall()
            return [row[0] for row in result]

    def start_ui(self, port: int=4213, open_browser: bool=True):
        if self.conn is None:
            raise ValueError('DuckDB connection is not established')
        _ = self.conn.execute(f"SET ui_local_port={port}")
        if open_browser:
            _ = self.conn.execute("CALL start_ui()")
        else:
            _ = self.conn.execute("CALL start_ui_server()")

    def stop_ui(self):
        if self.conn:
            _ = self.conn.execute("CALL stop_ui_server()")
