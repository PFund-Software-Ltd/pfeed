from __future__ import annotations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pfeed.typing import GenericFrame, StreamingData
    from pfeed.data_models.time_based_data_model import (
        TimeBasedDataModel,
        TimeBasedMetadataModel,
    )
    from pfeed.streaming_settings import StreamingSettings
    from pfeed._io.base_io import BaseIO
    from pfeed.utils.file_path import FilePath

import datetime
from pathlib import Path
from abc import abstractmethod

import pandas as pd
import polars as pl
import pyarrow as pa
from pydantic import Field

from pfeed.enums import DataTool
from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseMetadata
from pfeed.messaging.streaming_message import StreamingMessage
from pfeed.stream_buffer import StreamBuffer


class TimeBasedMetadata(BaseMetadata):
    file_metadatas: dict[FilePath, TimeBasedMetadataModel]
    missing_dates_at_source: list[datetime.date] = Field(
        description="Dates within the requested data period where no data exists at the source (e.g. non-trading days, holidays)."
    )
    missing_dates_in_storage: list[datetime.date] = Field(
        description="Dates within the requested data period that have data at the source but do not exist in local storage."
    )


class TimeBasedDataHandler(BaseDataHandler):
    PARTITION_COLUMNS = [
        "year",
        "month",
        "day",
    ]  # used by e.g. Delta Lake for partitioning

    def __init__(
        self,
        data_model: TimeBasedDataModel,
        data_path: FilePath,
        io: BaseIO,
        streaming_settings: StreamingSettings | None = None,
    ):
        super().__init__(data_model=data_model, data_path=data_path, io=io)
        self._stream_buffer: StreamBuffer | None = None
        if streaming_settings:
            self._create_stream_buffer(streaming_settings)

    @abstractmethod
    def _create_filename(self, date: datetime.date) -> str:
        pass

    @abstractmethod
    def _create_storage_path(self, date: datetime.date | None = None) -> Path:
        pass

    def _create_file_path(self, date: datetime.date | None = None) -> FilePath:
        if not self._is_using_table_format():
            assert date is not None, "date is required for non-table format"
            return (
                self._data_path
                / self._create_storage_path(date)
                / self._create_filename(date)
            )
        else:
            return self._data_path / self._create_storage_path()

    def create_file_paths(self) -> list[FilePath]:
        if not self._is_using_table_format():
            file_paths = [
                self._create_file_path(date=date) for date in self._data_model.dates
            ]
        else:
            file_paths = [self._create_file_path()]
        return file_paths

    def _create_stream_buffer(self, streaming_settings: StreamingSettings):
        if self._is_using_streaming_io():
            buffer_path: FilePath = self._create_file_path()
            self._stream_buffer = StreamBuffer(
                self._io, buffer_path, streaming_settings
            )
        else:
            raise ValueError(
                f"Streaming is not supported for {self._io.__class__.__name__}"
            )

    def write(
        self,
        data: GenericFrame | StreamingData,
        streaming: bool = False,
        validate: bool = True,
    ):
        if streaming:
            self._write_stream(data)
        else:
            self._write_batch(data, validate=validate)

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

        # add year, month, day columns for delta table partitioning
        data["year"] = date.year
        data["month"] = date.month
        data["day"] = date.day
        return data

    def _write_stream(self, data: StreamingData):
        data = self._standardize_streaming_msg(data)
        self._stream_buffer.write(
            data,
            metadata=self._data_model.to_metadata(),
            partition_by=self.PARTITION_COLUMNS,
        )

    def _write_batch(self, df: GenericFrame, validate: bool = True):
        from pfeed._etl.base import convert_to_desired_df

        df: pd.DataFrame = convert_to_desired_df(df, DataTool.pandas)
        # validate before writing data
        if validate:
            df = self._validate_schema(df)

        data_model: TimeBasedDataModel = self._data_model

        # split data with a date range into chunks per date
        if not self._is_using_table_format():
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
                file_path = self._create_file_path(date=date)
                if "index" in df_chunk.columns:
                    df_chunk.drop(columns=["index"], inplace=True)
                data = pa.Table.from_pandas(df_chunk, preserve_index=False)
                file_metadata: TimeBasedMetadataModel = data_model_copy.to_metadata()
                self._io.write(data=data, file_path=file_path, metadata=file_metadata)
        else:
            # Preprocess table to add year, month, day columns for partitioning
            df = df.assign(
                year=df["date"].dt.year,
                month=df["date"].dt.month,
                day=df["date"].dt.day,
            )
            table_path = self._create_file_path()
            metadata: TimeBasedMetadata = self.read_metadata()
            table_metadata: TimeBasedMetadataModel = data_model.to_metadata()
            existing_table_metadata: TimeBasedMetadataModel | None = (
                metadata.file_metadatas.get(table_path, None)
            )
            if existing_table_metadata:
                existing_dates = existing_table_metadata.dates
                # merge the current data model's metadata "dates" with the existing table metadata "dates"
                table_metadata.dates = list(set(existing_dates + table_metadata.dates))
                # Replace any overlapping data within the date range
                start_ts, end_ts = df["date"].min(), df["date"].max()
                predicate = f"date >= '{start_ts}' AND date <= '{end_ts}'"
            else:
                predicate = None
            data = pa.Table.from_pandas(df, preserve_index=False)
            self._io.write(
                data=data,
                file_path=table_path,
                metadata=table_metadata,
                predicate=predicate,
                partition_by=self.PARTITION_COLUMNS,
            )

    def read_metadata(self) -> TimeBasedMetadata:
        metadata: BaseMetadata = super().read_metadata()
        if not self._is_using_table_format():
            existing_dates = [
                date 
                for file_metadata in metadata.file_metadatas.values()
                for date in file_metadata.dates 
            ]
            # placeholder files that exist but are empty, as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
            missing_dates_at_source = [
                date
                for date in self._data_model.dates
                if date in existing_dates 
                and self._io.is_empty(self._create_file_path(date=date))
            ]
        else:
            table_path = self._create_file_path()
            table_metadata: TimeBasedMetadataModel | None = metadata.file_metadatas.get(
                table_path, None
            )
            existing_dates = table_metadata.dates if table_metadata else []
            missing_dates_at_source = [
                date
                for date in self._data_model.dates
                # if exists in table metadata but is empty = no data on that date (e.g. weekends, holidays, etc.)
                if date in existing_dates
                and self._io.is_empty(
                    table_path,
                    partition_filters=[
                        ("year", "=", date.year),
                        ("month", "=", date.month),
                        ("day", "=", date.day),
                    ],
                )
            ]
        return TimeBasedMetadata(
            **metadata.model_dump(),
            missing_dates_at_source=missing_dates_at_source,
            missing_dates_in_storage=[
                date for date in self._data_model.dates if date not in existing_dates
            ],
        )

    def read(self, **io_kwargs) -> pl.LazyFrame | None:
        file_paths = self.create_file_paths()
        lf: pl.LazyFrame | None = self._io.read(file_paths=file_paths, **io_kwargs)
        # drop e.g. year, month, day columns which are only used for partitioning
        if lf is not None:
            lf = lf.drop(self.PARTITION_COLUMNS)
        return lf
