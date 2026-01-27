from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import GenericFrame
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from pfeed._io.base_io import MetadataModelAsDict
    from duckdb import DuckDBPyConnection

import json

import pyarrow.fs as pa_fs
import polars as pl
import duckdb

from pfeed._io.database_io import DatabaseIO, DBPath
from pfeed._io.file_io import FileIO
from pfeed.enums import TimestampPrecision
from pfeed.storages.duckdb_storage import DuckDBStorage


class DuckDBIO(DatabaseIO, FileIO):
    # TODO: support streaming
    # SUPPORTS_STREAMING: bool = True
    SUPPORTS_PARALLEL_WRITES: bool = False
    FILE_EXTENSION = ".duckdb"
    TIMESTAMP_PRECISION: TimestampPrecision = TimestampPrecision.MICROSECOND

    def __init__(
        self,
        in_memory: bool=DuckDBStorage.DEFAULT_IN_MEMORY,
        memory_limit: str=DuckDBStorage.DEFAULT_MEMORY_LIMIT,
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(),
        storage_options: dict | None = None,
        io_options: dict | None = None,
    ):
        DatabaseIO.__init__(self, storage_options=storage_options, io_options=io_options)
        FileIO.__init__(self, filesystem=filesystem, storage_options=storage_options, io_options=io_options)
        self._in_memory = in_memory
        self._memory_limit = memory_limit
    
    def _open_connection(self, uri: str):
        self._conn_uri = uri
        self._conn: DuckDBPyConnection = duckdb.connect(uri, **self._io_options)
        if self._in_memory:
            if ";" in self._memory_limit or "'" in self._memory_limit:
                raise ValueError(f"Invalid memory_limit: {self._memory_limit!r}")
            self._conn.execute(f"SET memory_limit = '{self._memory_limit}'")
    
    def _close_connection(self):
        if self._conn:
            self._conn.close()
    
    def exists(self, db_path: DBPath) -> bool:
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            schema_name, table_name = schema_qualified_table_name.split('.')
            return conn.execute("""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = ?
                AND table_name = ?
            """, (schema_name, table_name)).fetchone() is not None
        except Exception:
            return False
    
    def is_empty(self, db_path: DBPath) -> bool:
        schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
        try:
            conn = self.connect(db_path.db_uri)
            # More efficient than COUNT(*) - just check if any row exists
            result = conn.execute(f"""
                SELECT 1 FROM {schema_qualified_table_name} LIMIT 1
            """).fetchone()
            return result is None
        except Exception:
            return True

    def write(self, data: GenericFrame, db_path: DBPath, delete_where: str | None = None):
        """Write data to DuckDB table using delete-then-insert pattern.

        Args:
            data: Data to write (pandas/polars DataFrame or PyArrow table).
            db_path: Database path specifying schema and table.
            delete_where: SQL WHERE clause to delete existing rows before inserting.
                If None, data is appended (may cause duplicates if called twice).
                If provided, matching rows are deleted first, then new data is inserted.
                Example: "date >= '2024-01-01' AND date <= '2024-01-31'"

        Note:
            This is NOT a filter for SELECT - it controls which rows to DELETE
            before inserting, enabling idempotent writes for a given predicate.
        """
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            schema_name, table_name = schema_qualified_table_name.split('.')
            
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            
            # Create a table with the same structure as df but without data by using WHERE 1=0 trick
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_qualified_table_name} AS 
                SELECT * FROM data WHERE 1=0
            """)
            
            # Delete any overlapping data before inserting
            if delete_where:
                conn.execute(f"""
                    DELETE FROM {schema_qualified_table_name} 
                    WHERE {delete_where}
                """)
            
            conn.execute(f"INSERT INTO {schema_qualified_table_name} SELECT * FROM data")
        except Exception as exc:
            raise Exception(
                f'Failed to write data (type={type(data)}) ({db_path=}): {exc}'
            ) from exc
    
    def write_metadata(self, db_path: DBPath, metadata: BaseMetadataModel) -> None:
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            schema_name, table_name = schema_qualified_table_name.split('.')
            metadata_table = f"{schema_name}.{self.METADATA_TABLE_NAME}"
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            # NOTE: 'updated_at' is an extra field added to the metadata
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {metadata_table} (
                    table_name VARCHAR PRIMARY KEY,
                    metadata_json JSON,
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            metadata_json = json.dumps(metadata.model_dump(), default=str)
            conn.execute(f"""
                INSERT OR REPLACE INTO {metadata_table} (table_name, metadata_json) VALUES (?, ?)
            """, (
                table_name,
                metadata_json,
            ))
        except Exception as exc:
            raise Exception(
                f'Failed to write metadata (type={type(metadata)}) ({db_path=}): {exc}'
            ) from exc
       
    def read(self, db_path: DBPath) -> pl.LazyFrame | None:
        lf: pl.LazyFrame | None = None
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            if self.exists(db_path):
                result = conn.execute(f"""
                    SELECT * FROM {schema_qualified_table_name} 
                """)
                # REVIEW: .pl(lazy=True) will lead to: INTERNAL Error: Attempted to dereference shared_ptr that is NULL!
                # when using narwhals to call nw.from_native(lf).head(1).collect()
                # use lazy=False for now
                # lf = result.pl(lazy=True)
                lf = result.pl(lazy=False).lazy()
            return lf
        except Exception as exc:
            raise Exception(
                f'Failed to read data ({db_path=}): {exc}'
            ) from exc
    
    def read_metadata(self, db_path: DBPath) -> dict[DBPath, MetadataModelAsDict]:
        metadata: dict[DBPath, MetadataModelAsDict] = {}
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            schema_name, table_name = schema_qualified_table_name.split('.')
            metadata_table = f"{schema_name}.{self.METADATA_TABLE_NAME}"
            metadata_db_path = DBPath(
                db_uri=db_path.db_uri,
                db_name=db_path.db_name,
                schema_name=db_path.schema_name,
                table_name=self.METADATA_TABLE_NAME,
            )
            if self.exists(metadata_db_path):
                result = conn.execute(f"""
                    SELECT metadata_json FROM {metadata_table}
                    WHERE table_name = ?
                """, (table_name,)).fetchone()
                if result is not None:
                    metadata[db_path] = json.loads(result[0])
        except Exception as exc:
            raise Exception(
                f'Failed to read metadata ({db_path=}): {exc}'
            ) from exc
        return metadata
