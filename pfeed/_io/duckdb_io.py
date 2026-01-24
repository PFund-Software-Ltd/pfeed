from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfeed.utils.file_path import FilePath
    from pfeed.typing import GenericFrame
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from pfeed._io.base_io import MetadataModelAsDict
    from duckdb import DuckDBPyConnection

import json

import pyarrow.fs as pa_fs
import pandas as pd
import polars as pl
import duckdb

from pfeed._io.database_io import DatabaseIO, DBPath, SchemaQualifiedTableName
from pfeed._io.file_io import FileIO
from pfeed.enums import TimestampPrecision
from pfeed.storages.duckdb_storage import DuckDBStorage


class DuckDBIO(DatabaseIO, FileIO):
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
    
    def _create_uri(self, data_path: FilePath, db_name: str) -> str:
        return ':memory:' if self._in_memory else f'{data_path}/{db_name}{self.FILE_EXTENSION}'
    
    def _open_connection(self, uri: str):
        self._conn_uri = uri
        self._conn: DuckDBPyConnection = duckdb.connect(uri, **self._io_options)
        if self._in_memory:
            self._conn.execute(f"SET memory_limit = '{self._memory_limit}'")
    
    def _close_connection(self):
        self._conn.close()

    # REVIEW: make it SQL-safe
    def _sanitize_table_name(self, table_name: str) -> str:
        return (
            table_name
            .replace('-', '_')
            .replace(':', '_')
            .replace('.', 'p')  # 123456.123 -> 123456p123, e.g. for strike price
        )
    
    def exists(self, db_path: DBPath) -> bool:
        try:
            conn = self._connect(db_path.db_uri)
            schema_name = db_path.schema_name
            table_name = db_path.table_name
            return conn.execute(f"""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
            """).fetchone() is not None
        except Exception:
            return False
    
    # TODO
    def is_empty(self, schema_name: str, table_name: str) -> bool:
        schema_qualified_table_name = f"{schema_name}.{table_name}"

    def write(self, data: GenericFrame, db_path: DBPath, where: str | None = None):
        try:
            conn = self._connect(db_path.db_uri)
            schema_qualified_table_name = f"{db_path.schema_name}.{db_path.table_name}"
            
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {db_path.schema_name}")
            
            # Create a table with the same structure as df but without data by using WHERE 1=0 trick
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_qualified_table_name} AS 
                SELECT * FROM data WHERE 1=0
            """)
            
            # Delete any overlapping data before inserting
            if where:
                conn.execute(f"""
                    DELETE FROM {schema_qualified_table_name} 
                    WHERE {where}
                """)
            
            conn.execute(f"INSERT INTO {schema_qualified_table_name} SELECT * FROM data")
        except Exception as exc:
            raise Exception(
                f'Failed to write data (type={type(data)}) ({db_path=}): {exc}'
            ) from exc
    
    def write_metadata(self, db_path: DBPath, metadata: BaseMetadataModel) -> None:
        schema_name = db_path.schema_name
        table_name = db_path.table_name
        schema_qualified_table_name = f"{schema_name}.{self.METADATA_TABLE_NAME}"
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        # NOTE: 'updated_at' is an extra field added to the metadata
        self._conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_qualified_table_name} (
                table_name VARCHAR PRIMARY KEY,
                metadata_json JSON,
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        metadata_json = json.dumps(metadata.model_dump(), default=str)
        self._conn.execute(f"""
            INSERT OR REPLACE INTO {schema_qualified_table_name} (table_name, metadata_json) VALUES (?, ?)
        """, (
            table_name,
            metadata_json,
        ))
    
    def read(self, schema_name: str, table_name: str) -> tuple[pl.LazyFrame | None, StorageMetadata]:
        schema_qualified_table_name = f"{schema_name}.{table_name}"
        try:
            with self:
                lf: pl.LazyFrame | None = self._read_df(schema_qualified_table_name)
                metadata: StorageMetadata = self._read_storage_metadata()
                return lf, metadata
        except Exception as exc:
            raise Exception(
                f'Failed to read data ({schema_qualified_table_name=}): {exc}'
            ) from exc
    
    def _read_df(self, schema_name: str, table_name: str) -> pl.LazyFrame | None:
        schema_qualified_table_name = f"{schema_name}.{table_name}"
        table_name = schema_qualified_table_name.split('.')[-1]
        if not self.exists(table_name=table_name):
            return None
        start_date, end_date = self.data_model.start_date, self.data_model.end_date
        conn = self._conn.execute(f"""
            SELECT * FROM {schema_qualified_table_name} 
            WHERE CAST(date AS DATE) 
            BETWEEN CAST('{start_date}' AS DATE) AND CAST('{end_date}' AS DATE) 
            ORDER BY date
        """)
        lf: pl.LazyFrame = conn.pl(lazy=True)
        return lf
    
    def read_metadata(self, schema_name: str) -> DuckDBMetadata | dict:
        '''Reads metadata from duckdb metadata table'''
        table_name = self.METADATA_TABLE_NAME
        schema_qualified_table_name = f"{schema_name}.{table_name}"
        if not self.exists(table_name=table_name):
            return {}
        conn = self._conn.execute(f"""
            SELECT * FROM {schema_qualified_table_name}
            WHERE table_name = '{table_name}'
        """)
        metadata: DuckDBMetadata = conn.df().to_dict(orient='records')
        if not metadata:
            return {}
        else:
            metadata = metadata[0]
            metadata['dates'] = [date.item().date() for date in metadata['dates']]
            return metadata

    # TEMP: remove this method
    def read_metadata_json(self, schema_name: str, table_name: str) -> dict[SchemaQualifiedTableName, MetadataModelAsDict]:
        """Read custom application metadata stored as JSON in duckdb metadata table.

        Args:
            schema_name: Schema name where metadata table is located.
            table_names: List of table names to read metadata for.

        Returns:
            Dictionary mapping table names to their metadata dicts.
        """
        metadata: dict[SchemaQualifiedTableName, MetadataModelAsDict] = {}
        if not self.exists(schema_name=schema_name, table_name=self.METADATA_TABLE_NAME):
            return metadata
        schema_qualified_table_name = f"{schema_name}.{self.METADATA_TABLE_NAME}"
        for table_name in table_names:
            result = self._conn.execute(f"""
                SELECT metadata_json FROM {schema_qualified_table_name}
                WHERE table_name = ?
            """, (table_name,)).fetchone()
            if result and result[0]:
                metadata[table_name] = json.loads(result[0])
        return metadata
        
    def _read_storage_metadata(self) -> StorageMetadata:
        '''Reads metadata from duckdb metadata table and consolidates it to follow other storages metadata structure'''
        storage_metadata: StorageMetadata = {}
        duckdb_metadata: DuckDBMetadata | dict = self.read_metadata()
        storage_metadata['file_metadata'] = {self.filename: duckdb_metadata}
        if isinstance(self.data_model, TimeBasedDataModel):
            if 'dates' in duckdb_metadata:
                existing_dates = duckdb_metadata['dates']
                storage_metadata['missing_dates'] = [date for date in self.data_model.dates if date not in existing_dates]
            else:
                storage_metadata['missing_dates'] = self.data_model.dates
        else:
            raise NotImplementedError(f'{type(self.data_model)=}')
        return storage_metadata


# TEMP
if __name__ == '__main__':
    import duckdb
    from pfeed import get_config
    config = get_config()
    data_path = config.data_path / 'test'
    io = DuckDBIO(conn=duckdb.connect(data_path + DuckDBIO.FILE_EXTENSION))
    io.write(
        schema_name='test',
        table_name='test',
        data=pd.DataFrame({
            'date': [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
            'value': [1, 2],
        }),
    )
