from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from pfeed.typing import GenericFrame, StreamingData
    from pfeed.data_models.time_based_data_model import (
        TimeBasedDataModel,
        TimeBasedMetadataModel,
    )
    from pfeed.streaming_settings import StreamingSettings
    from pfeed._io.base_io import BaseIO
    from pfeed.utils.file_path import FilePath
    from pfeed.data_handlers.base_data_handler import SourcePath

import datetime
from pathlib import Path
from abc import abstractmethod

import polars as pl
import pyarrow as pa
from pydantic import Field

from pfund_kit.style import cprint, TextStyle, RichColor
from pfeed.enums import DataTool, DataLayer, TimestampPrecision
from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseMetadata
from pfeed.messaging.streaming_message import StreamingMessage
from pfeed.stream_buffer import StreamBuffer


class TimeBasedMetadata(BaseMetadata):
    source_metadata: dict[SourcePath, TimeBasedMetadataModel]
    missing_dates_in_storage: list[datetime.date] = Field(
        description="Dates within the requested data period that have data at the source but do not exist in local storage."
    )
    # missing_dates_at_source: list[datetime.date] = Field(
    #     description="Dates within the requested data period where no data exists at the source (e.g. non-trading days, holidays)."
    # )


class TimeBasedDataHandler(BaseDataHandler):
    PARTITION_COLUMNS = ["date"]  # used by e.g. Delta Lake for partitioning

    def __init__(
        self,
        data_path: Path,
        data_layer: DataLayer,
        data_model: TimeBasedDataModel,
        io: BaseIO,
    ):
        super().__init__(data_path=data_path, data_layer=data_layer, data_model=data_model, io=io)
        self._stream_buffer: StreamBuffer | None = None
        self._file_paths_per_date = {
            date: self._create_file_path(date=date) for date in data_model.dates
        } if self._is_file_io() else {}
        self._file_paths: list[FilePath] = list(self._file_paths_per_date.values())

    @abstractmethod
    def _create_file_path(self, date: datetime.date) -> FilePath:
        pass

    def create_stream_buffer(self, streaming_settings: StreamingSettings):
        if self._is_streaming_io():
            # EXTEND: only support deltalake_io for now
            if self._is_table_io():
                buffer_path = self._table_path
                self._stream_buffer = StreamBuffer(
                    self._io, buffer_path, streaming_settings
                )
            # TODO: writing streaming data to database is not supported yet
            else:
                raise ValueError(f"Streaming is not supported for {self._io.name}")
        else:
            raise ValueError(f"Streaming is not supported for {self._io.name}")

    def write(
        self,
        data: GenericFrame | StreamingData,
        streaming: bool = False,
        validate: bool = True,
        **io_kwargs,
    ):
        if streaming:
            self._write_stream(data)
        else:
            self._write_batch(data, validate=validate, **io_kwargs)

    # NOTE: streaming data (env=LIVE/PAPER) does NOT follow the same columns in write_batch (env=BACKTEST)
    def _standardize_streaming_msg(self, msg: StreamingMessage) -> dict:
        """
        Convert StreamingMessage to dict and standardize it, e.g. convert timestamp to datetime (UTC)
        """
        data = msg.to_dict()

        # drop empty dicts
        dict_fields = ["extra_data", "custom_data"]
        for field in dict_fields:
            if not data[field]:
                data.pop(field)

        # flatten specs, make each spec a column, e.g. strike_price is now a column
        for k, v in data["specs"].items():
            data[k] = v
        data.pop("specs")

        # convert timestamp to datetime (UTC)
        date = datetime.datetime.fromtimestamp(
            data["ts"], tz=datetime.timezone.utc
        ).replace(tzinfo=None)
        data["date"] = date

        return data

    # EXTEND: currently only supports writing for parquet+deltalake (using .arrow for buffering)
    def _write_stream(self, data: StreamingData):
        data = self._standardize_streaming_msg(data)
        self._stream_buffer.write(
            data,
            metadata=self._data_model.to_metadata(),
            partition_by=self.PARTITION_COLUMNS,
        )

    def _write_batch(self, df: GenericFrame, validate: bool = True, **io_kwargs):
        import pandas as pd
        from pandas.api.types import is_datetime64_ns_dtype
        from pfeed._etl.base import convert_to_desired_df

        df: pd.DataFrame = convert_to_desired_df(df, DataTool.pandas)
        # validate before writing data
        if validate:
            df = self._validate_schema(df)

        data_model: TimeBasedDataModel = self._data_model

        # split data with a date range into chunks per date
        if self._is_file_io():
            if df.empty:
                data_chunks_per_date = {}
            else:
                data_chunks_per_date = {
                    date: group for date, group in df.groupby(df["date"].dt.date)
                }
            # split data model spanning multiple dates into separate data models, each with a single date
            for date in data_model.dates:
                data_model_copy = data_model.model_copy(deep=False)
                # NOTE: create placeholder data if date is not in data_chunks_per_date,
                # used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
                df_chunk = data_chunks_per_date.get(
                    date, pd.DataFrame(columns=df.columns)
                )
                # make date range (start_date, end_date) to (date, date), since storage is per date
                data_model_copy.update_start_date(date)
                data_model_copy.update_end_date(date)
                file_path = self._file_paths_per_date[date]
                if "index" in df_chunk.columns:
                    df_chunk.drop(columns=["index"], inplace=True)
                table = pa.Table.from_pandas(df_chunk, preserve_index=False)
                file_metadata: TimeBasedMetadataModel = data_model_copy.to_metadata()
                # NOTE: this only writes metadata to the table schema, not to the file.
                table_with_metadata = self._io.write_metadata(table, file_metadata)
                self._io.write(data=table_with_metadata, file_path=file_path, **io_kwargs)
        elif self._is_table_io() or self._is_database_io():
            if self._is_table_io():
                source_path = self._table_path
                if self._io.SUPPORTS_PARTITIONING:
                    io_kwargs["partition_by"] = self.PARTITION_COLUMNS
            elif self._is_database_io():
                source_path = self._db_path
                if df.empty:
                    cprint(f'Empty DataFrame (db_path={source_path})', style=str(TextStyle.BOLD + RichColor.RED))
                    return
                # convert datetime64[ns] to lower precision if db doesn't support nanosecond precision
                if self._io.TIMESTAMP_PRECISION < TimestampPrecision.NANOSECOND and is_datetime64_ns_dtype(df['date'].dtype):
                    df['date'] = df['date'].astype('datetime64[us]')
                    cprint(f"Converting 'date' column from NANOSECOND precision to {self._io.TIMESTAMP_PRECISION} for compatibility in {self._io.name}")
            
            source_metadata: TimeBasedMetadataModel = data_model.to_metadata()
            existing_metadata: TimeBasedMetadata = self.read_metadata()
            existing_source_metadata: TimeBasedMetadataModel | None = existing_metadata.source_metadata.get(
                source_path, None
            )
            if existing_source_metadata:
                existing_dates = existing_source_metadata.dates
                # merge the current data model's metadata "dates" with the existing table metadata "dates"
                source_metadata.dates = list(set(existing_dates + source_metadata.dates))
                # Replace any overlapping data within the date range
                start_ts, end_ts = df["date"].min(), df["date"].max()
                where = f"date >= '{start_ts}' AND date <= '{end_ts}'"
            else:
                where = None
            data = pa.Table.from_pandas(df, preserve_index=False)
            self._io.write(
                data,
                source_path,
                where=where,
                **io_kwargs,
            )
            self._io.write_metadata(source_path, source_metadata)
        else:
            raise ValueError(f"Unsupported IO format: {self._io.name}")

    def read_metadata(self) -> TimeBasedMetadata:
        '''Reads all metadata from storage based on the data model'''
        metadata: BaseMetadata = super().read_metadata()
        if self._is_file_io():
            existing_dates = [
                date 
                for file_metadata in metadata.source_metadata.values()
                for date in file_metadata.dates 
            ]
        elif self._is_table_io() or self._is_database_io():
            source_path = self._table_path if self._is_table_io() else self._db_path
            source_metadata: TimeBasedMetadataModel | None = metadata.source_metadata.get(
                source_path, None
            )
            existing_dates = source_metadata.dates if source_metadata else []
        else:
            raise ValueError(f"Unsupported IO format: {self._io.name}")
        return TimeBasedMetadata(
            **metadata.model_dump(),
            missing_dates_in_storage=[
                date for date in self._data_model.dates if date not in existing_dates
            ],
        )

    def read(self, **io_kwargs) -> pl.LazyFrame | None:
        if self._is_file_io():
            return self._io.read(file_paths=self._file_paths, **io_kwargs)
        elif self._is_table_io():
            return self._io.read(table_path=self._table_path, **io_kwargs)
        elif self._is_database_io():
            return self._io.read(db_path=self._db_path, **io_kwargs)
        else:
            raise ValueError(f"Unsupported IO format: {self._io.name}")
