'''
default non-duckdb storage structure:
- env/data_source/unique_identifier/product_type/product_name/resolution/year/month/{filename}.parquet
VS
duckdb storage structure:
- .duckdb file per env/data_source/unique_identifier/
- schema: product_type/
- one table per product_name/resolution, where year/month is removed
'''
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

import json
from pathlib import Path

import pandas as pd
from pandas.api.types import is_datetime64_ns_dtype
import duckdb

from pfund import print_warning
from pfeed.typing.core import is_dataframe
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.storages.base_storage import BaseStorage
from pfeed import get_config


config = get_config()


class DuckDBStorage(BaseStorage):
    conn: DuckDBPyConnection
    
    _schema_name: str
    _table_name: str
    _in_memory: bool = False

    def __post_init__(self):
        super().__post_init__()
        self._schema_name = self._create_schema_name()
        self._table_name = self._create_table_name()
        self._in_memory = self.kwargs.get('in_memory', False)
        if self._in_memory:
            self.conn = duckdb.connect(':memory:')
        elif self.file_path.exists():
            self.conn = duckdb.connect(str(self.file_path))
        else:
            self.conn = None
    
    def _create_schema_name(self) -> str:
        if isinstance(self.data_model, MarketDataModel):
            return self.data_model.product.type.value.lower()
        else:
            raise NotImplementedError(f'{type(self.data_model)=}')
    
    def _create_table_name(self) -> str:
        if isinstance(self.data_model, MarketDataModel):
            return (
                # '_' is SQL-safe, and is used as a separator in the table name
                # NOTE: use str(resolution) instead of repr() to avoid case-sensitive table names, e.g. '1m' and '1M'
                '_'.join([self.data_model.product.name.lower(), str(self.data_model.resolution).lower()])
                .replace('-', '_')
                .replace(':', '_')
                .replace('.', 'p')  # 123456.123 -> 123456p123, e.g. for strike price
            )
        else:
            raise NotImplementedError(f'{type(self.data_model)=}')
    
    @property
    def _schema_table_name(self) -> str:
        return f'{self._schema_name}.{self._table_name}'
        
    @property
    def _metadata_schema_table_name(self) -> str:
        return f'{self._schema_name}.metadata'
    
    @property
    def file_extension(self) -> str:
        return '.duckdb'
    
    @property
    def filename(self) -> str:
        name = self.data_model.source.name.lower()
        if self.data_model.is_unique_identifier_effective():
            name = self.data_model.unique_identifier.lower()
        return name + self.file_extension
    
    @property
    def storage_path(self) -> Path:
        return (
            Path(self.data_model.env.value) 
            / self.data_model.source.name 
            / self.data_model.unique_identifier
            / self.filename
        )
    
    def _create_data_path(self) -> Path | str | None:
        return None if self._in_memory else super()._create_data_path()
    
    def _create_file_path(self) -> Path | str | None:
        return None if self._in_memory else super()._create_file_path()
        
    def exists(self) -> bool:
        return super().exists() and self._table_exists() and self._data_exists()

    def _data_exists(self) -> bool:
        '''Checks if data exists in the table for specified date(s).
        Data is considered to exist if either:
        1. The data is present in the table
        2. A placeholder date exists in the metadata for that date
        '''
        if not self._table_exists():
            return False
        df: pd.DataFrame = self.get_table(full_table=True)
        metadata = self.get_metadata(include_placeholder_dates=True)
        placeholder_dates: list[str] = metadata['placeholder_dates']
        dates_in_table: list[str] = df['ts'].dt.strftime('%Y-%m-%d').unique().tolist() if not df.empty else []
        duplicated_dates = set(dates_in_table) & set(placeholder_dates)
        # This is a rare edge case, occurs when data for a date was missing
        # during the initial download but becomes available in a subsequent download
        if duplicated_dates:
            self._remove_placeholder_dates(metadata, duplicated_dates)
        total_dates = dates_in_table + placeholder_dates
        # check if all dates in the data model are present in the table
        return all(str(date) in total_dates for date in self.data_model.dates)

    def _remove_placeholder_dates(self, metadata: dict, dates_to_remove: list[str]):
        print_warning(f"""
            Removing placeholder dates: {dates_to_remove} - This should not occur. 
            It may indicate a bug in pfeed, an issue with the data provider's API, 
            or a network-related problem during data download.
            It is generally safe to ignore this warning.
        """)
        metadata['placeholder_dates'] = [date for date in metadata['placeholder_dates'] if date not in dates_to_remove]
        self._write_metadata(metadata)

    def _conn_exists(self) -> bool:
        return self.conn is not None
    
    def _table_exists(self, table_name: str | Literal['metadata']='') -> bool:
        table_name = table_name or self._table_name
        if not self._conn_exists():
            return False
        try:
            return self.conn.execute(f"""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = '{self._schema_name}'
                AND table_name = '{table_name}'
            """).fetchone() is not None
        except Exception:
            return False
    
    def _adjust_metadata(self, metadata: dict) -> dict:
        '''Converts is_placeholder to placeholder_dates since duckdb storage is not per date'''
        if 'is_placeholder' in metadata:
            metadata['placeholder_dates'] = []
            if metadata['is_placeholder'] == 'true':
                placeholder_date = str(self.data_model.start_date)
                metadata['placeholder_dates'] = [placeholder_date]
            metadata.pop('is_placeholder', None)
        return metadata
        
    def write_table(self, df: pd.DataFrame, metadata: dict):
        if not is_dataframe(df):
            raise ValueError(f'{type(df)=} is not a dataframe, cannot write to duckdb')
        if not self._conn_exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(str(self.file_path))
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema_name}")
        if not df.empty:
            if 'ts' not in df.columns:
                raise ValueError("DataFrame must have a 'ts' column.")
            # duckdb doesn't support datetime64[ns]
            if is_datetime64_ns_dtype(df['ts'].dtype):
                df['ts'] = df['ts'].astype('datetime64[us]')
                if config.print_msg:
                    print_warning(f"Converting 'ts' column from datetime64[ns] to datetime64[us] for {self.name} {self.data_model} compatibility")
            
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {self._schema_table_name} AS SELECT * FROM df")
            # Delete any overlapping data within the date range before inserting
            start_date, end_date = self.data_model.start_date, self.data_model.end_date
            self.conn.execute(f"""
                DELETE FROM {self._schema_table_name} 
                WHERE CAST(ts AS DATE) BETWEEN CAST('{start_date}' AS DATE) AND CAST('{end_date}' AS DATE)
            """)
            self.conn.execute(f"INSERT INTO {self._schema_table_name} SELECT * FROM df")
        metadata = self._adjust_metadata(metadata)
        if existing_metadata := self.get_metadata(include_placeholder_dates=True):
            metadata['placeholder_dates'] += existing_metadata['placeholder_dates']
            metadata['placeholder_dates'] = list(set(metadata['placeholder_dates']))
        self._write_metadata(metadata)
    
    def _write_metadata(self, metadata: dict):
        if not self._conn_exists():
            self.conn = duckdb.connect(str(self.file_path))
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema_name}")
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._metadata_schema_table_name} (
                table_name VARCHAR,
                metadata_json JSON,
                PRIMARY KEY (table_name)
            )
        """)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO {self._metadata_schema_table_name} (table_name, metadata_json)
            VALUES (?, ?)
        """, [self._table_name, metadata])
    
    def get_table(self, full_table: bool=False) -> pd.DataFrame:
        """
        Args:
            full_table: whether to get the full table or just the date range of the data model
        """
        if not self._table_exists(table_name=self._table_name):
            return None
        if not full_table:
            start_date, end_date = self.data_model.start_date, self.data_model.end_date
            conn = self.conn.execute(f"SELECT * FROM {self._schema_table_name} WHERE CAST(ts AS DATE) BETWEEN CAST'{start_date}' AS DATE) AND CAST('{end_date}' AS DATE) ORDER BY ts")
        else:
            conn = self.conn.execute(f"SELECT * FROM {self._schema_table_name} ORDER BY ts")
        # REVIEW: this should return pl.LazyFrame ideally if duckdb supports it    
        df: pd.DataFrame = conn.df()
        return df
    
    def get_metadata(self, include_placeholder_dates: bool=False) -> dict:
        '''
        Args:
            include_placeholder_dates: whether to include key 'placeholder_dates' in the metadata
            'placeholder_dates' is a key specific to duckdb storage, since duckdb storage is not per date
            and cannot write an empty dataframe for a date that has no data
        '''
        if not self._table_exists(table_name='metadata'):
            return {}
        try:
            result = self.conn.execute(f"""
                SELECT metadata_json 
                FROM {self._metadata_schema_table_name} 
                WHERE table_name = '{self._table_name}'
            """).fetchone()
            metadata = json.loads(result[0]) if result else {}
            if not include_placeholder_dates:
                metadata.pop('placeholder_dates', None)
            return metadata
        except Exception:
            return {}

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
    
    def close(self):
        if self._conn_exists():
            self.conn.close()
    
    def __enter__(self):
        return self  # Setup - returns the object to be used in 'with'
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()  # Cleanup - always runs at end of 'with' block
    
    def __del__(self):
        """Ensure connection is closed when object is garbage collected"""
        if self._conn_exists():
            self.close()
