from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pathlib import Path

from pfeed import get_config
from pfeed.enums import IOFormat
from pfeed.storages.database_storage import DatabaseStorage
from pfeed._io.duckdb_io import DuckDBIO


config = get_config()


class DuckDBStorage(DatabaseStorage):
    SUPPORTED_IO_FORMATS = [IOFormat.DUCKDB]
    DEFAULT_IN_MEMORY = True
    DEFAULT_MEMORY_LIMIT = '4GB'
    
    # TODO: add __new__ to determine if inherit from LocalStorage or S3Storage?
    def __new__(cls, *args, in_memory: bool=DEFAULT_IN_MEMORY, memory_limit: str=DEFAULT_MEMORY_LIMIT, **kwargs):
        pass

    def _create_uri(self) -> str:
        return ':memory:' if self._in_memory else self.data_path
    
    def with_io(
        self, 
        in_memory: bool=DEFAULT_IN_MEMORY,
        memory_limit: str=DEFAULT_MEMORY_LIMIT,
        io_options: dict | None = None,
        **kwargs,  # Unused parameters accepted for compatibility with other storage backends
    ) -> DuckDBIO:
        return super().with_io(in_memory=in_memory, memory_limit=memory_limit, io_options=io_options)
    
    def _create_io(self, in_memory: bool=DEFAULT_IN_MEMORY, memory_limit: str=DEFAULT_MEMORY_LIMIT, io_options: dict | None = None) -> DuckDBIO:
        return DuckDBIO(
            in_memory=in_memory,
            memory_limit=memory_limit,
            filesystem=self.get_filesystem(),
            storage_options=self.storage_options,
            io_options=io_options,
        )
    
    # FIXME
    @staticmethod
    def get_duckdb_files() -> list[Path]:
        duckdb_files = []
        for file_path in (config.data_path.parent / 'duckdb').rglob('**/*.duckdb'):
            duckdb_files.append(file_path)
        return duckdb_files
    
    def show_tables(self, include_schema: bool=False) -> list[tuple[str, str] | str]:
        if include_schema:
            result: list[tuple[str, str]] = self.conn.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
            """).fetchall()
        else:
            result: list[tuple[str]] = self.conn.execute(f"""
                SELECT table_name 
                FROM information_schema.tables
                WHERE table_schema = '{self._schema_name}'
            """).fetchall()
        return [row[0] if not include_schema else (row[0], row[1]) for row in result]
    
    def start_ui(self, port: int=4213, no_browser: bool=False):
        self.conn.execute(f"SET ui_local_port={port}")
        if not no_browser:
            self.conn.execute("CALL start_ui()")
        else:
            self.conn.execute("CALL start_ui_server()")
        
    def stop_ui(self):
        self.conn.execute("CALL stop_ui_server()")
    