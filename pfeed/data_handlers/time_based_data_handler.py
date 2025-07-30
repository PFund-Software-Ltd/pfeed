from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.messaging.streaming_message import StreamingMessage
    from pfeed.typing import GenericFrame, StorageMetadata
    from pfeed.data_models.time_based_data_model import (
        TimeBasedDataModel, 
        TimeBasedMetadata, 
        TimeBasedDeltaMetadata
    )

import time
import atexit
import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa
try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None

from pfeed.enums import DataLayer, StreamMode
from pfeed.data_handlers.base_data_handler import BaseDataHandler
from pfeed.storages.deltalake_storage_mixin import DeltaLakeStorageMixin


class TimeBasedDataHandler(BaseDataHandler):
    DELTA_PARTITION_BY = ['year', 'month', 'day']  # delta table partition by
    # REVIEW: make it a param?
    DELTA_FLUSH_INTERVAL = 100  # in seconds, flush streaming data to deltalake every x seconds
    
    def __init__(
        self, 
        data_model: TimeBasedDataModel,
        data_layer: DataLayer,
        data_path: str,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None = None,
        use_deltalake: bool = False,
        stream_mode: StreamMode=StreamMode.FAST,
    ):
        '''
        Args:
            data_path: data path that already consists of data layer and data domain
                but still has no information about the data model
            stream_mode: SAFE or FAST
                if "FAST" is chosen, streaming data will be cached to memory to a certain amount before writing to disk,
                faster write speed, but data loss risk will increase.
                if "SAFE" is chosen, streaming data will be written to disk immediately,
                slower write speed, but data loss risk will be minimized.
        '''
        from pfeed.adapter import Adapter
        from pfeed._io.buffer_io import BufferIO
        from pfeed._io.tabular_io import TabularIO
        
        super().__init__(
            data_model=data_model, 
            data_layer=data_layer, 
            data_path=data_path,
            storage_options=storage_options,
            use_deltalake=use_deltalake,
        )
        self._io = TabularIO(
            filesystem=filesystem,
            storage_options=storage_options,
            use_deltalake=use_deltalake,
        )
        self._buffer_io = BufferIO(
            filesystem=filesystem,
            storage_options=storage_options,
            stream_mode=stream_mode,
        )
        self._buffer_path: Path = Path(self._create_file_paths()[0]) / self._buffer_io.BUFFER_FILENAME
        self._buffer_io._mkdir(self._buffer_path)
        self._last_delta_flush = time.time()
        self._adapter = Adapter()
        self._message_schemas: dict[Path, pa.Schema] = {}
        self._recover_from_crash()
        atexit.register(self._cleanup_at_exit)
    
    # FIXME: this function is supposed to just call _write_buffer_to_deltalake()
    # but somehow it could cause error when reading buffer.arrow: Memory pointer from external source (e.g, FFI) is not aligned with the specified scalar type.
    def _recover_from_crash(self):
        '''
        Recover from crash by reading buffer.arrow and writing it to deltalake
        This is only needed when the _cleanup_at_exit is not called, e.g. program crashes
        '''
        # Error description: Memory pointer from external source (e.g, FFI) is not aligned with the specified scalar type.
        # The Arrow array’s memory starts at an address that isn’t a proper multiple (e.g., 8 bytes) for its data type.
        # Rust Arrow requires that alignment for safe, fast reads; when it’s wrong, it panics to avoid undefined behavior.
        # see https://arrow.apache.org/rust/src/arrow_buffer/buffer/scalar.rs.html
        # see https://github.com/delta-io/delta-rs/issues/3407
        # HACK: current workaround is to convert the table to pandas and back to table
        table = self._buffer_io.read(self._buffer_path)
        table = pa.Table.from_pandas(table.to_pandas(), preserve_index=False, schema=table.schema)
        self._io.write(
            table=table,
            file_path=self._buffer_path.parent,
            metadata=self._data_model.to_metadata(),
            delta_partition_by=self.DELTA_PARTITION_BY,
        )
        self._buffer_io.clear_disk(self._buffer_path)
        
    def _cleanup_at_exit(self):
        self._write_buffer_to_deltalake()
        
    # FIXME: being used as a better type hint, fix this
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def _create_file_paths(self, data_model: TimeBasedDataModel | None=None) -> list[str]:
        data_model = data_model or self._data_model
        if not self._use_deltalake:
            file_paths = [
                '/'.join([
                    self._data_path, 
                    str(data_model.create_storage_path(date, use_deltalake=self._use_deltalake)), 
                    data_model.create_filename(date)
                ])
                for date in data_model.dates
            ]
        else:
            file_paths = ['/'.join([self._data_path, str(data_model.create_storage_path(data_model.dates[0], use_deltalake=self._use_deltalake))])]
        return file_paths
    
    def write(self, data: GenericFrame | StreamingMessage, streaming: bool=False):
        if streaming:
            self._write_stream(data)
        else:
            self._write_batch(data)
            
    # NOTE: streaming data (env=LIVE/PAPER) does NOT follow the same columns in write_batch (env=BACKTEST)
    def _create_streaming_data(self, msg: StreamingMessage) -> dict:
        '''
        Convert StreamingMessage to dict and standardize it, e.g. convert timestamp to datetime (UTC)
        '''
        streaming_buffer = msg.to_dict()
        
        # drop empty dicts
        dict_fields = ['extra_data', 'custom_data']
        for field in dict_fields:
            if not streaming_buffer[field]:
                streaming_buffer.pop(field)
        
        # flatten specs, make each spec a column, e.g. strike_price is now a column
        for k, v in streaming_buffer['specs'].items():
            streaming_buffer[k] = v
        streaming_buffer.pop('specs')

        # convert timestamp to datetime (UTC)
        date = datetime.datetime.fromtimestamp(
            streaming_buffer['ts'],
            tz=datetime.timezone.utc
        ).replace(tzinfo=None)
        streaming_buffer['date'] = date

        # add year, month, day columns for delta table partitioning
        streaming_buffer['year'] = date.year
        streaming_buffer['month'] = date.month
        streaming_buffer['day'] = date.day
        return streaming_buffer

    def _infer_message_schema(self, streaming_data: dict) -> pa.Schema:
        if self._buffer_path not in self._message_schemas:
            self._message_schemas[self._buffer_path] = self._adapter.dict_to_schema(streaming_data)
        return self._message_schemas[self._buffer_path]
    
    def _write_buffer_to_deltalake(self):
        self._buffer_io.spill_to_disk(self._buffer_path, self._message_schemas[self._buffer_path])
        self._io.write(
            table=self._buffer_io.read(self._buffer_path),
            file_path=self._buffer_path.parent,
            metadata=self._data_model.to_metadata(),
            delta_partition_by=self.DELTA_PARTITION_BY,
        )
        self._buffer_io.clear_disk(self._buffer_path)
        
    def _write_stream(self, msg: StreamingMessage):
        streaming_data = self._create_streaming_data(msg)
        schema = self._infer_message_schema(streaming_data)
        self._buffer_io.write(self._buffer_path, streaming_data, schema)
        now = time.time()
        if now - self._last_delta_flush > self.DELTA_FLUSH_INTERVAL:
            self._write_buffer_to_deltalake()
            self._last_delta_flush = now
        
    def _write_batch(self, df: GenericFrame):
        from pfeed._etl.base import convert_to_pandas_df

        DeltaTableMetadata: TimeBasedDeltaMetadata | dict[str, Any]

        df: pd.DataFrame = convert_to_pandas_df(df)
        # reset index to avoid pandera.errors.SchemaError: DataFrameSchema failed series or dataframe validator 0: <Check validate_index_reset>
        df = df.reset_index(drop=True)
        df = self._validate_schema(df)

        data_model: TimeBasedDataModel = self._data_model
        # split data with a date range into chunks per date
        data_chunks_per_date = {} if df.empty else {date: group for date, group in df.groupby(df['date'].dt.date)}
        if not self._use_deltalake:
            for date in data_model.dates:
                data_model_copy = data_model.model_copy(deep=False)
                # NOTE: create placeholder data if date is not in data_chunks_per_date, 
                # used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
                df_chunk = data_chunks_per_date.get(date, pd.DataFrame(columns=df.columns))
                # make date range (start_date, end_date) to (date, date), since storage is per date
                data_model_copy.update_start_date(date)
                data_model_copy.update_end_date(date)
                file_path = self._create_file_paths(data_model=data_model_copy)[0]
                table = pa.Table.from_pandas(df_chunk, preserve_index=False)
                self._io.write(table, file_path, metadata=data_model_copy.to_metadata())
        else:            
            # Preprocess table to add year, month, day columns for partitioning
            df = df.assign(
                year=df["date"].dt.year,
                month=df["date"].dt.month,
                day=df["date"].dt.day,
            )
            
            file_path = self._create_file_paths(data_model=data_model)[0]
            is_deltatable = DeltaTable.is_deltatable(file_path, storage_options=self._storage_options)
            existing_dates = []
            if is_deltatable:
                metadata_file_path = file_path + '/' + DeltaLakeStorageMixin.metadata_filename
                existing_metadata: dict[str, Any] = self._io._read_pyarrow_table_metadata([metadata_file_path])
                file_metadata: DeltaTableMetadata = existing_metadata.get(metadata_file_path, {})
                existing_dates = file_metadata.get('dates', [])
                existing_dates = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in existing_dates]

                # Delete any overlapping data within the date range before inserting
                dt = DeltaTable(file_path, storage_options=self._storage_options)
                start_ts, end_ts = df['date'].iloc[0], df['date'].iloc[-1]
                dt.delete(predicate=f"date >= '{start_ts}' AND date <= '{end_ts}'")
            
            metadata: TimeBasedMetadata = data_model.to_metadata()
            dates_in_data = pd.date_range(metadata.pop('start_date'), metadata.pop('end_date')).date.tolist()
            total_dates = list(set(dates_in_data + existing_dates))
            metadata['dates'] = total_dates
            table = pa.Table.from_pandas(df, preserve_index=False)
            self._io.write(table, file_path, metadata=metadata, delta_partition_by=self.DELTA_PARTITION_BY)

    def read(self, delta_version: int | None=None) -> tuple[pl.LazyFrame | None, StorageMetadata]:
        lf, metadata = self._io.read(file_paths=self._create_file_paths(), delta_version=delta_version)
        data_model: TimeBasedDataModel = self._data_model
        missing_file_paths: list[str] = metadata['missing_file_paths']
        if not self._use_deltalake:
            missing_dates = [fp.split('/')[-1].rsplit('_', 1)[-1].rsplit('.', 1)[0] for fp in missing_file_paths]
            missing_dates = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in missing_dates]
            metadata['missing_dates'] = missing_dates
        else:
            # drop e.g. year, month, day columns which are only used for partitioning
            lf = lf.drop(self.DELTA_PARTITION_BY)
            file_metadata = metadata['file_metadata']
            deltalake_metadata = list(file_metadata.values())[0]
            existing_dates = deltalake_metadata.get('dates', [])
            existing_dates = [datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in existing_dates]
            metadata['missing_dates'] = [date for date in data_model.dates if date not in existing_dates]
        return lf, metadata
