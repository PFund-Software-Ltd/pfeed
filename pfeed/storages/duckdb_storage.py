'''
default non-duckdb storage structure:
- data_layer/data_domain/env/data_source/data_origin/product_type/product_name/resolution/year/month/day/{filename}.parquet
VS
duckdb storage structure:
- .duckdb file per data_layer/data_domain/env/data_source/data_origin/
- schema: product_type/
- one table per product_name/resolution, where year/month/day is removed
'''
from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Any
from typing_extensions import TypedDict
if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from pfeed.typing import tDATA_TOOL, tDATA_LAYER
    from pfeed.typing import GenericFrame
    from pfeed.data_models.base_data_model import BaseDataModel

import datetime
from pathlib import Path

import pandas as pd
from pandas.api.types import is_datetime64_ns_dtype
import duckdb

from pfund import print_warning
from pfeed.data_models.time_based_data_model import TimeBasedDataModel
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.storages.base_storage import BaseStorage
from pfeed.enums import DataTool
from pfeed import get_config


config = get_config()


class DuckDBMetadata(TypedDict):
    table_name: str  # product_name_resolution, e.g. aapl_usd_stk_1_day
    dates: list[datetime.date]
    updated_at: datetime.datetime
    

# EXTEND: only supports MarketDataModel for now
class DuckDBStorage(BaseStorage):
    def __init__(
        self,
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='general_data',
        in_memory: bool=False, 
        memory_limit: str='4GB',
        **kwargs
    ):
        '''
        Args:
            in_memory: whether to use in-memory storage
        '''
        if 'use_deltalake' in kwargs:
            kwargs.pop('use_deltalake')
        self._in_memory = in_memory
        self._memory_limit = memory_limit
        super().__init__(
            name='duckdb', 
            data_layer=data_layer,
            data_domain=data_domain,
            **kwargs
        )
        self._schema_name = ''
        self._table_name = ''
        self._metadata_table_name = 'metadata'
        self.conn: DuckDBPyConnection | None = None
    
    def _create_connection(self) -> DuckDBPyConnection:
        if self._in_memory:
            conn: DuckDBPyConnection = duckdb.connect(':memory:', **self._kwargs)
            conn.execute(f"SET memory_limit = '{self._memory_limit}'")
        else:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            conn: DuckDBPyConnection = duckdb.connect(str(self.file_path), **self._kwargs)
        return conn
    
    @classmethod
    def from_data_model(
        cls,
        data_model: BaseDataModel, 
        data_layer: tDATA_LAYER,
        data_domain: str,
        in_memory: bool=False,
        memory_limit: str='4GB',
        **kwargs
    ) -> BaseStorage:
        if 'use_deltalake' in kwargs:
            kwargs.pop('use_deltalake')
        instance = cls(
            data_layer=data_layer,
            data_domain=data_domain,
            in_memory=in_memory,
            memory_limit=memory_limit,
            **kwargs
        )
        instance.attach_data_model(data_model)
        instance.initialize_logger()
        instance._schema_name = instance._create_schema_name()
        instance._table_name = instance._create_table_name()
        instance.conn = instance._create_connection()
        return instance
    
    @staticmethod
    def get_duckdb_files() -> list[Path]:
        duckdb_files = []
        for file_path in (Path(config.data_path).parent / 'duckdb').rglob('**/*.duckdb'):
            duckdb_files.append(file_path)
        return duckdb_files
    
    def get_filesystem(self):
        raise NotImplementedError('DuckDBStorage does not support filesystem operations')
    
    def _create_schema_name(self) -> str:
        if isinstance(self.data_model, MarketDataModel):
            return self.data_model.product.type.value.lower()
        else:
            raise NotImplementedError(f'{type(self.data_model)=}')
    
    def _create_table_name(self) -> str:
        if isinstance(self.data_model, MarketDataModel):
            return (
                # '_' is SQL-safe, and is used as a separator in the table name
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
        return f'{self._schema_name}.{self._metadata_table_name}'
    
    @property
    def data_path(self) -> Path | None:
        if self._in_memory:
            return None
        else:
            return (
                Path(config.data_path).parent 
                / self.name.lower()
                / f'data_layer={self.data_layer.name.lower()}'
                / f'data_domain={self.data_domain}'
            )
    
    # NOTE: only duckdb has filename, file_path and storage_path; for other storages, they are handled by data handler
    @property
    def filename(self) -> str:
        name = self.data_model.data_source.name.lower()
        if self.data_model.is_data_origin_effective():
            name = self.data_model.data_origin.lower()
        return name + '.duckdb'
    
    @property
    def file_path(self) -> Path | str | None:
        return self.data_path / self.storage_path / self.filename if not self._in_memory else None
    
    @property
    def storage_path(self) -> Path | None:
        return (
            Path(f'env={self.data_model.env.value}')
            / f'data_source={self.data_model.data_source.name}'
            / f'data_origin={self.data_model.data_origin}'
        )
    
    def _conn_exists(self) -> bool:
        return self.conn is not None

    def table_exists(self, table_name: str | Literal['metadata']='') -> bool:
        table_name = table_name or self._table_name
        try:
            return self.conn.execute(f"""
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = '{self._schema_name}'
                AND table_name = '{table_name}'
            """).fetchone() is not None
        except Exception:
            return False
        
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
    
    def _write_df(self, df: pd.DataFrame):
        if 'date' not in df.columns:
            raise ValueError("DataFrame must have a 'date' column.")

        # duckdb doesn't support datetime64[ns]
        if is_datetime64_ns_dtype(df['date'].dtype):
            df['date'] = df['date'].astype('datetime64[us]')
            print_warning(f"Converting 'date' column from datetime64[ns] to datetime64[us] for {self.name} {self.data_model} compatibility")

        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema_name}")
        
        # Create a table with the same structure as df but without data by using WHERE 1=0 trick
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._schema_table_name} AS 
            SELECT * FROM df WHERE 1=0
        """)
        
        # Delete any overlapping data within the date range before inserting
        start_ts, end_ts = df['date'].iloc[0], df['date'].iloc[-1]
        self.conn.execute(f"""
            DELETE FROM {self._schema_table_name} 
            WHERE date >= '{start_ts}' AND date <= '{end_ts}'
        """)
        
        self.conn.execute(f"INSERT INTO {self._schema_table_name} SELECT * FROM df")
    
    def _read_df(self) -> pd.DataFrame | None:
        if not self.table_exists(table_name=self._table_name):
            return None
        start_date, end_date = self.data_model.start_date, self.data_model.end_date
        conn = self.conn.execute(f"""
            SELECT * FROM {self._schema_table_name} 
            WHERE CAST(date AS DATE) 
            BETWEEN CAST('{start_date}' AS DATE) AND CAST('{end_date}' AS DATE) 
            ORDER BY date
        """)
        df: pd.DataFrame = conn.df()
        return df

    def _write_metadata(self, metadata: DuckDBMetadata) -> None:
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema_name}")
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._metadata_schema_table_name} (
                table_name VARCHAR PRIMARY KEY,
                dates DATE[],
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        self.conn.execute(f"""
            INSERT OR REPLACE INTO {self._metadata_schema_table_name} (table_name, dates) VALUES (?, ?)
        """, (
            metadata['table_name'],
            metadata['dates'],
        ))
    
    def _read_metadata(self) -> DuckDBMetadata | dict:
        '''Reads metadata from duckdb metadata table'''
        if not self.table_exists(table_name=self._metadata_table_name):
            return {}
        try:
            conn = self.conn.execute(f"""
                SELECT * FROM {self._metadata_schema_table_name}
                WHERE table_name = '{self._table_name}'
            """)
            metadata: DuckDBMetadata = conn.df().to_dict(orient='records')
            if not metadata:
                return {}
            else:
                metadata = metadata[0]
                metadata['dates'] = [date.item().date() for date in metadata['dates']]
                return metadata
        except Exception:
            self._logger.exception(f'Failed to read metadata from {self.name} using table_name={self._table_name}')
            return {}
        
    def _read_storage_metadata(self) -> dict[str, Any]:
        '''Reads metadata from duckdb metadata table and consolidates it to follow other storages metadata structure'''
        storage_metadata: dict[str, Any] = {}
        duckdb_metadata: DuckDBMetadata | dict = self._read_metadata()
        storage_metadata['file_metadata'] = {self.filename: duckdb_metadata}
        if isinstance(self.data_model, TimeBasedDataModel):
            if 'dates' in duckdb_metadata:
                existing_dates_in_duckdb = duckdb_metadata['dates']
                storage_metadata['missing_dates'] = [date for date in self.data_model.dates if date not in existing_dates_in_duckdb]
            else:
                storage_metadata['missing_dates'] = self.data_model.dates
        else:
            raise NotImplementedError(f'{type(self.data_model)=}')
        return storage_metadata

    def write_data(self, data: GenericFrame) -> bool:
        from pfeed._etl.base import convert_to_pandas_df
        try:
            with self:
                df: pd.DataFrame = convert_to_pandas_df(data)
                if df.empty:
                    self._logger.warning(f'Empty DataFrame for {self.name} {self.data_model}')
                    return False
                self._write_df(df)
                current_metadata: DuckDBMetadata = self._read_metadata()
                current_dates_in_storage = current_metadata.get('dates', [])
                total_dates = list(set(self.data_model.dates + current_dates_in_storage))
                metadata: DuckDBMetadata = {'table_name': self._table_name, 'dates': total_dates}
                self._write_metadata(metadata)
                return True
        except Exception:
            self._logger.exception(f'Failed to write data (type={type(data)}) to {self.name}')
            return False
    
    def read_data(self, data_tool: DataTool | tDATA_TOOL='polars') -> tuple[GenericFrame | None, DuckDBMetadata]:
        from pfeed._etl.base import convert_to_user_df
        try:
            with self:
                df: pd.DataFrame | None = self._read_df()
                metadata: dict[str, Any] = self._read_storage_metadata()
                data_tool = DataTool[data_tool.lower()] if isinstance(data_tool, str) else data_tool
                if df is not None:
                    df: GenericFrame = convert_to_user_df(df, data_tool)
                return df, metadata
        except Exception:
            self._logger.exception(f'Failed to read data ({data_tool=}) (table_name={self._table_name}) from {self.name}')
            return None, {}
    
    def start_ui(self, port: int=4213, no_browser: bool=False):
        self.conn.execute(f"SET ui_local_port={port}")
        if not no_browser:
            self.conn.execute("CALL start_ui()")
        else:
            self.conn.execute("CALL start_ui_server()")
        
    def stop_ui(self):
        self.conn.execute("CALL stop_ui_server()")
    
    def close(self):
        if self._conn_exists():
            self.conn.close()
    
    def __enter__(self):
        return self  # Setup - returns the object to be used in 'with'
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()  # Cleanup - always runs at end of 'with' block
    
    def __del__(self):
        """Ensure connection is closed when object is garbage collected"""
        self.close()
