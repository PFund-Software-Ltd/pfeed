'''
default non-duckdb storage structure:
- env/data_source/data_origin/product_type/product_name/resolution/year/month/{filename}.parquet
VS
duckdb storage structure:
- .duckdb file per env/data_source/data_origin/
- schema: product_type/
- one table per product_name/resolution, where year/month is removed
'''
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from pfeed.typing.literals import tDATA_TOOL, tDATA_LAYER
    from pfeed.typing.core import tDataFrame

import json
from pathlib import Path

import pandas as pd
from pandas.api.types import is_datetime64_ns_dtype
import duckdb

from pfund import print_warning
from pfeed.utils.dataframe import is_dataframe
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.storages.base_storage import BaseStorage
from pfeed.const.enums import DataTool
from pfeed import get_config


config = get_config()


class DuckDBStorage(BaseStorage):
    def __init__(
        self,
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='general_data',
        use_deltalake: bool=False,
        in_memory: bool=True, 
        **kwargs
    ):
        '''
        Args:
            in_memory: whether to use in-memory storage
        '''
        self._in_memory = in_memory
        super().__init__(
            name='duckdb', 
            data_layer=data_layer,
            data_domain=data_domain,
            use_deltalake=use_deltalake,
            **kwargs
        )
        self._schema_name = self._create_schema_name()
        self._table_name = self._create_table_name()
        if self._in_memory:
            self.conn: DuckDBPyConnection = duckdb.connect(':memory:', **self._kwargs)
        elif self.file_path.exists():
            self.conn: DuckDBPyConnection = duckdb.connect(str(self.file_path), **self._kwargs)
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
    def filename(self) -> str | None:
        if not self._data_model:
            return None
        name = self._data_model.source.name.lower()
        if self._data_model.is_data_origin_effective():
            name = self._data_model.data_origin.lower()
        return name + self._data_model.file_extension
    
    @property
    def file_path(self) -> Path | str | None:
        return None if self._in_memory else super().file_path

    @property
    def storage_path(self) -> Path | None:
        if not self._data_model:
            return None
        return (
            Path(self.data_layer)
            / self.data_domain
            / self.data_model.env.value
            / self.data_model.source.name 
            / self.data_model.data_origin
        )
    
    @property
    def data_path(self) -> Path | str | None:
        return None if self._in_memory else super().data_path
    
    def _exists(self) -> bool:
        return self.file_path.exists() and self._table_exists() and self._data_exists()

    def _data_exists(self) -> bool:
        '''Checks if data exists in the table for specified date(s).
        Data is considered to exist if either:
        1. The data is present in the table
        2. A placeholder date exists in the metadata for that date
        '''
        if not self._table_exists():
            return False
        df: pd.DataFrame = self.get_table()
        metadata = self._get_metadata(include_placeholder_dates=True)
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
    
    def _adjust_metadata(self, metadata: dict | None) -> dict:
        '''Converts is_placeholder to placeholder_dates since duckdb storage is not per date'''
        metadata = metadata or {}
        if 'is_placeholder' in metadata:
            metadata['placeholder_dates'] = []
            if metadata['is_placeholder'] == 'true':
                placeholder_date = str(self.data_model.start_date)
                metadata['placeholder_dates'] = [placeholder_date]
            metadata.pop('is_placeholder', None)
        return metadata
        
    def write_table(self, df: pd.DataFrame, metadata: dict | None=None):
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
        if existing_metadata := self._get_metadata(include_placeholder_dates=True):
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
    
    def get_table(self) -> pd.DataFrame:
        if not self._table_exists(table_name=self._table_name):
            return None
        start_date, end_date = self.data_model.start_date, self.data_model.end_date
        conn = self.conn.execute(f"SELECT * FROM {self._schema_table_name} WHERE CAST(ts AS DATE) BETWEEN CAST'{start_date}' AS DATE) AND CAST('{end_date}' AS DATE) ORDER BY ts")
        # REVIEW: this should return pl.LazyFrame ideally if duckdb supports it    
        df: pd.DataFrame = conn.df()
        return df
    
    def _get_metadata(self, include_placeholder_dates: bool=False) -> dict:
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

    def write_data(self, data: tDataFrame):
        from pfeed.etl import convert_to_pandas_df
        data: pd.DataFrame = convert_to_pandas_df(data)
        with self:
            metadata = {}
            data_chunks_per_date = {} if data.empty else {date: group for date, group in data.groupby(data['ts'].dt.date)}
            for date in self._data_model.dates:
                if date not in data_chunks_per_date:
                    metadata['is_placeholder'] = 'true'
                else:
                    metadata['is_placeholder'] = 'false'
            self.write_table(data, metadata=metadata)
    
    def read_data(self, data_tool: DataTool | tDATA_TOOL='pandas') -> tDataFrame | None:
        from pfeed.etl import convert_to_user_df
        if not self._exists():
            return None
        with self:
            df: pd.DataFrame = self.get_table()
            data_tool = DataTool[data_tool.upper()] if isinstance(data_tool, str) else data_tool
            df: tDataFrame = convert_to_user_df(df, data_tool)
            return df
    
    def __enter__(self):
        return self  # Setup - returns the object to be used in 'with'
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()  # Cleanup - always runs at end of 'with' block
    
    def __del__(self):
        """Ensure connection is closed when object is garbage collected"""
        if self._conn_exists():
            self.close()
