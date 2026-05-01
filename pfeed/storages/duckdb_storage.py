from __future__ import annotations
from typing import Any

from pfeed.enums import IOFormat, DataLayer, DataStorage
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.storages.database_storage import DatabaseStorage


class DuckDBStorage(DatabaseStorage):
    SUPPORTED_IO_FORMATS = [IOFormat.DUCKDB]
    DEFAULT_IN_MEMORY = False
    DEFAULT_MEMORY_LIMIT = '4GB'
    
    def __new__(cls, *args: Any, file_backend: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL, **kwargs: Any):
        FileBasedStorage = DataStorage[file_backend.upper()].storage_class
        new_cls = type(cls.__name__, (cls, FileBasedStorage), {'__module__': cls.__module__})
        return object.__new__(new_cls)
    
    def __init__(
        self, 
        data_path: str | None = None,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
        storage_options: dict[str, Any] | None = None,
        file_backend: FileBasedDataStorage | str = FileBasedDataStorage.LOCAL,
        **kwargs: Any,  # additional kwargs for compatibility with other storages
    ):
        if storage_options is None:
            storage_options = {
                'in_memory': self.DEFAULT_IN_MEMORY,
                'memory_limit': self.DEFAULT_MEMORY_LIMIT,
            }
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
            **kwargs,
        )
        if self.storage_options['in_memory']:
            self.data_path = ':memory:'
    
    def _create_uri(self) -> str:
        return ''

    def get_file_path(self) -> str:
        db_path = self._get_db_path()
        return db_path.db_uri
    
    def show_tables(self, include_schema: bool = True) -> list[str]:
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
        self.conn.execute(f"SET ui_local_port={port}")
        if open_browser:
            self.conn.execute("CALL start_ui()")
        else:
            self.conn.execute("CALL start_ui_server()")
        
    def stop_ui(self):
        if self.conn:
            self.conn.execute("CALL stop_ui_server()")
    