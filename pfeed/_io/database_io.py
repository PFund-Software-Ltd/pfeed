from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, NamedTuple
if TYPE_CHECKING:
    import polars as pl
    from pfeed._io.duckdb_io import DuckDBPyConnection
    from pfeed._io.postgresql_io import PostgresConnection, AsyncPostgresConnection
    from pfeed.typing import GenericFrame
    SchemaQualifiedTableName: TypeAlias = str
    DBConnection: TypeAlias = DuckDBPyConnection | PostgresConnection | AsyncPostgresConnection

from abc import abstractmethod

from pfeed._io.base_io import BaseIO
from pfeed.enums import TimestampPrecision


# this is not an actual path, but carrier of information to where the db is, including db name, schema name and table name etc.
# calling it a path is just for consistency with FilePath and TablePath
class DBPath(NamedTuple):
    # e.g. for postgresql, it is "postgresql://user:password@localhost:5432/{db_name}"
    # for duckdb, it is the data path to the .duckdb file, e.g. "/path/to/data/{db_name}.duckdb"
    db_uri: str
    db_name: str
    schema_name: str | None
    table_name: str


# NOTE: only support SQL databases
class DatabaseIO(BaseIO):
    TIMESTAMP_PRECISION: TimestampPrecision = TimestampPrecision.MICROSECOND
    METADATA_TABLE_NAME: str = "metadata"

    def __init__(self, storage_options: dict | None = None, io_options: dict | None = None):
        super().__init__(storage_options=storage_options, io_options=io_options)
        self._conn: DBConnection | None = None
        self._conn_uri: str | None = None
    
    def _get_schema_qualified_table_name(self, db_path: DBPath) -> SchemaQualifiedTableName:
        schema_name = self._sanitize_identifier(db_path.schema_name)
        table_name = self._sanitize_identifier(db_path.table_name)
        return f"{schema_name}.{table_name}"
    
    # REVIEW:
    def _sanitize_identifier(self, name: str) -> str:
        return (
            name
            .replace('-', '_')
            .replace(':', '_')
            .replace('.', 'p')    # 123456.123 -> 123456p123, e.g. for strike price
            .replace('/', '_')    # for crypto pairs like BTC/USDT
            .replace(' ', '_')    # spaces
            .replace(';', '_')    # prevent statement termination
            .replace("'", '_')    # prevent string breakout
            .replace('"', '_')    # prevent identifier breakout
            .replace('\\', '_')   # prevent escape sequences
            .replace('\x00', '')  # null byte
        )
    
    def connect(self, uri: str) -> DBConnection:
        if self._conn is None:
            self._open_connection(uri)
        else:
            # if its opening a different file
            if uri != self._conn_uri:
                self._close_connection()
                self._open_connection(uri)
        return self._conn
    
    def disconnect(self):
        """Explicitly close the database connection."""
        if self._conn is not None:
            self._close_connection()
            self._conn = None
            self._conn_uri = None
    
    @abstractmethod
    def write(self, data: GenericFrame, db_path: DBPath):
        pass
    
    @abstractmethod
    def read(self, db_path: DBPath) -> pl.LazyFrame | None:
        pass

    @abstractmethod
    def exists(self, db_path: DBPath) -> bool:
        pass

    @abstractmethod
    def is_empty(self, db_path: DBPath) -> bool:
        pass
    
    @abstractmethod
    def _open_connection(self, uri: str):
        pass

    @abstractmethod
    def _close_connection(self):
        pass

    def __enter__(self):
        return self  # Setup - returns the object to be used in 'with'
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()  # Cleanup - always runs at end of 'with' block
    
    def __del__(self):
        """Ensure connection is closed when object is garbage collected"""
        self.disconnect()
