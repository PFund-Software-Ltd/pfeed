from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pathlib import Path
    from pfeed._io.duckdb_io import DuckDBIO

from pfeed import get_config
from pfeed.enums import IOFormat, DataLayer, DataStorage
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.storages.database_storage import DatabaseStorage


config = get_config()


class DuckDBStorage(DatabaseStorage):
    SUPPORTED_IO_FORMATS = [IOFormat.DUCKDB]
    DEFAULT_IN_MEMORY = False
    DEFAULT_MEMORY_LIMIT = '4GB'
    
    def __new__(cls, *args, data_storage: FileBasedDataStorage = FileBasedDataStorage.LOCAL, **kwargs):
        FileBasedStorage = DataStorage[data_storage].storage_class
        new_cls = type(cls.__name__, (cls, FileBasedStorage), {'__module__': cls.__module__})
        return object.__new__(new_cls)
    
    def __init__(
        self, 
        data_path: Path | str | None = None,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str | Literal['MARKET_DATA', 'NEWS_DATA'] = 'MARKET_DATA',
        data_storage: FileBasedDataStorage = FileBasedDataStorage.LOCAL,
        storage_options: dict | None = None,
    ):
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )

    def _create_uri(self) -> str:
        return ''
    
    def with_io(
        self, 
        io_options: dict | None = None, 
        in_memory: bool=DEFAULT_IN_MEMORY,
        memory_limit: str=DEFAULT_MEMORY_LIMIT,
        **kwargs,
    ) -> DuckDBIO:
        '''
        Args:
            kwargs: Unused parameters accepted for compatibility with other storage backends
        '''
        if in_memory:
            self.data_path = ':memory:'
        return super().with_io(in_memory=in_memory, memory_limit=memory_limit, io_options=io_options)

    def _create_io(
        self, 
        in_memory: bool=DEFAULT_IN_MEMORY,
        memory_limit: str=DEFAULT_MEMORY_LIMIT,
        io_options: dict | None = None, 
        **kwargs,
    ) -> DuckDBIO:
        '''
        Args:
            kwargs: Unused parameters accepted for compatibility with other storage backends
        '''
        from pfeed._io.duckdb_io import DuckDBIO
        return DuckDBIO(
            in_memory=in_memory,
            memory_limit=memory_limit,
            filesystem=self.get_filesystem(),
            storage_options=self.storage_options,
            io_options=io_options,
        )

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
    