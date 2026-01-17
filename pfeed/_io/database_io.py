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
    
    @abstractmethod
    def _create_uri(self, *args, **kwargs) -> str:
        pass

    def _connect(self, uri: str) -> DBConnection:
        if self._conn is None:
            self._open_connection(uri)
        else:
            # if its opening a different file
            if uri != self._conn_uri:
                self._close_connection()
                self._open_connection(uri)
        return self._conn
    
    def create_db_path(self, db_uri: str, db_name: str, table_name: str, schema_name: str | None = None) -> DBPath:
        '''
        Create a DBPath object from the given parameters.
        Args:
            db_uri: the URI of the database
                e.g. for postgresql, it is "postgresql://user:password@localhost:5432"
                for duckdb, it is the data path to the .duckdb file
            db_name: the name of the database
                e.g. for postgresql, it could be "my_db"
                for duckdb, it is the name of the .duckdb file without the extension
            schema_name: the name of the schema
            table_name: the name of the table
        Returns:
            DBPath: the DBPath object
        '''
        return DBPath(db_uri=db_uri, db_name=db_name, schema_name=schema_name, table_name=table_name)
        
    @abstractmethod
    def write(self, db_path: DBPath, data: GenericFrame) -> bool:
        pass
    
    @abstractmethod
    def read(self, db_path: DBPath) -> pl.LazyFrame:
        pass
    
    @abstractmethod
    def _open_connection(self) -> DBConnection:
        pass

    @abstractmethod
    def _close_connection(self):
        pass