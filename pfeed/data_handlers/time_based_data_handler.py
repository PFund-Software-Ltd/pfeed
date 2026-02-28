from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    import pandas as pd
    from pfeed.storages.database_storage import DatabaseURI
    from pfeed.enums import StreamMode
    from pfeed.typing import GenericFrame, StreamingData
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel
    from pfeed._io.base_io import BaseIO
    from pfeed.utils.file_path import FilePath

import datetime
from abc import abstractmethod, ABC

import polars as pl
import pyarrow as pa
from pydantic import Field

from pfund_kit.style import cprint, TextStyle, RichColor
from pfeed.enums import DataTool, DataLayer, TimestampPrecision
from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseMetadata
from pfeed.streaming.streaming_message import StreamingMessage
from pfeed.streaming.stream_buffer import StreamBuffer
from pfeed.data_handlers.base_data_handler import SourcePath
from pfeed.data_models.time_based_data_model import TimeBasedMetadataModel


class TimeBasedMetadata(BaseMetadata):
    source_metadata: dict[SourcePath, TimeBasedMetadataModel]
    missing_dates_in_storage: list[datetime.date] = Field(
        description="Dates within the requested data period that have data at the source but do not exist in local storage."
    )
    # missing_dates_at_source: list[datetime.date] = Field(
    #     description="Dates within the requested data period where no data exists at the source (e.g. non-trading days, holidays)."
    # )

    def to_dict(self) -> dict[str, Any]:
        return {
            **super().to_dict(),
            "missing_dates_in_storage": self.missing_dates_in_storage,
        }


class TimeBasedDataHandler(BaseDataHandler, ABC):
    PARTITION_COLUMNS: ClassVar[list[str]] = ["year", "month", "day"]  # used by e.g. Delta Lake for partitioning

    def __init__(
        self,
        data_path: FilePath | DatabaseURI,
        data_layer: DataLayer,
        data_domain: str,
        data_model: TimeBasedDataModel,
        io: BaseIO,
    ):
        '''
        Args:
            data_path: data path provided by file based storage
                e.g. for local storage, data_path = config.data_path
                it is None for database based storage
        '''
        super().__init__(data_path=data_path, data_layer=data_layer, data_domain=data_domain, data_model=data_model, io=io)
        self._stream_buffer: StreamBuffer | None = None
        self._file_paths_per_date = {
            date: self._create_file_path(date=date) for date in data_model.dates
        } if self._is_file_io() else {}
        self._file_paths: list[FilePath] = list(self._file_paths_per_date.values())

    @abstractmethod
    def _create_file_path(self, date: datetime.date) -> FilePath:
        pass

    def create_stream_buffer(self, stream_mode: StreamMode, flush_interval: int):
        if self._is_streaming_io():
            # EXTEND: only support deltalake_io for now
            if self._is_table_io():
                buffer_path = self._table_path
                assert buffer_path is not None, 'buffer_path is required for table io'
                self._stream_buffer = StreamBuffer(
                    io=self._io, 
                    buffer_path=buffer_path, 
                    stream_mode=stream_mode, 
                    flush_interval=flush_interval
                )
            # TODO: writing streaming data to database is not supported yet
            else:
                raise ValueError(f"Streaming is not supported for {self._io.name}")
        else:
            raise ValueError(f"Streaming is not supported for {self._io.name}")

    def write(self, data: GenericFrame | StreamingData, streaming: bool = False, **io_kwargs: Any):
        if streaming:
            self._write_stream(data)
        else:
            with self._io:
                self._write_batch(data, **io_kwargs)

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
        if self._supports_partitioning():
            data["year"] = date.year
            data["month"] = date.month
            data["day"] = date.day

        return data
    
    def _drop_temporary_date_column_in_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        '''Clean up temporary date column in raw data'''
        from pfeed.feeds.time_based_feed import TimeBasedFeed
        if self._data_layer == DataLayer.RAW:
            original_date_column = TimeBasedFeed.original_date_column_in_raw_data
            if original_date_column in df.columns:
                df['date'] = df[original_date_column]
                df.drop(columns=[original_date_column], inplace=True)
            else:
                df.drop(columns=['date'], inplace=True)
        return df

    # EXTEND: currently only supports writing for parquet+deltalake (using .arrow for buffering)
    def _write_stream(self, data: StreamingData):
        # TODO: check to ensure data is not raw
        print(f'***WRITE STREAM GOT*** {data}')
        # data =I self._standardize_streaming_msg(data)
        # self._stream_buffer.write(
        #     data,
        #     metadata=self._data_model.to_metadata(),
        #     partition_by=self.PARTITION_COLUMNS,
        # )
    
    # FIXME: currently hardcoded for deltalake_io, need to generalize
    def _requires_partitioning(self) -> bool:
        return self._io.name == 'DeltaLakeIO'

    def _write_batch(self, df: GenericFrame, **io_kwargs: Any):
        import pandas as pd
        from pandas.api.types import is_datetime64_ns_dtype
        from pfeed._etl.base import convert_to_desired_df

        df: pd.DataFrame = convert_to_desired_df(df, DataTool.pandas)
        is_raw_data = self._data_layer == DataLayer.RAW
        # validate before writing data
        if not is_raw_data:
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
                df_chunk = self._drop_temporary_date_column_in_raw_data(df_chunk)
                table = pa.Table.from_pandas(df_chunk, preserve_index=False)
                file_metadata: TimeBasedMetadataModel = data_model_copy.to_metadata()
                # NOTE: this only writes metadata to the table schema, not to the file.
                table_with_metadata = self._io.write_metadata(table, file_metadata)
                self._io.write(data=table_with_metadata, file_path=file_path, **io_kwargs)
        elif self._is_table_io() or self._is_database_io():
            if self._is_table_io():
                source_path = self._table_path
                # Preprocess table to add year, month, day columns for partitioning
                if self._requires_partitioning():
                    df = df.assign(
                        year=df["date"].dt.year,
                        month=df["date"].dt.month,
                        day=df["date"].dt.day,
                    )
                    io_kwargs["partition_by"] = self.PARTITION_COLUMNS
            elif self._is_database_io():
                source_path = self._db_path
                if df.empty:
                    cprint(f'Empty DataFrame (db_path={source_path})', style=TextStyle.BOLD + RichColor.RED)
                    return
                # convert datetime64[ns] to lower precision if db doesn't support nanosecond precision
                if self._io.TIMESTAMP_PRECISION < TimestampPrecision.NANOSECOND and is_datetime64_ns_dtype(df['date'].dtype):
                    df['date'] = df['date'].astype('datetime64[us]')
                    cprint(
                        f"\nConverting 'date' column from NANOSECOND precision to {self._io.TIMESTAMP_PRECISION} for compatibility in {self._io.name}",
                        style=TextStyle.BOLD
                    )
            
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
            if not is_raw_data:  # raw data does NOT have a date column, so can't delete any data
                delete_where = self._get_delete_where(df)
            else:
                delete_where = None

            df = self._drop_temporary_date_column_in_raw_data(df)
            data = pa.Table.from_pandas(df, preserve_index=False)
            self._io.write(
                data,
                source_path,
                delete_where=delete_where,
                **io_kwargs,
            )
            self._io.write_metadata(source_path, source_metadata)
        else:
            raise ValueError(f"Unsupported IO format: {self._io.name}")
    
    def _get_delete_where(self, df: pd.DataFrame) -> str:
        """Generate a delete filter for overlapping date ranges with IO-specific syntax.

        Constructs a WHERE clause to delete existing rows that overlap with the date range
        of the incoming data. Handles IO-specific syntax differences, particularly timestamp
        casting requirements for LanceDBIO.

        Args:
            df: DataFrame containing a 'date' column with timestamp data.

        Returns:
            Filter expression string for deleting overlapping rows, or None if no filter needed.
            - LanceDBIO: Uses explicit timestamp casting (e.g., "date >= cast('2025-01-01' as timestamp)")
            - Other IOs: Uses standard SQL syntax (e.g., "date >= '2025-01-01'")
        """
        start_ts, end_ts = df["date"].min(), df["date"].max()
        if self._io.name == 'LanceDBIO':
            return f"date >= cast('{start_ts}' as timestamp) AND date <= cast('{end_ts}' as timestamp)"
        else:
            return f"date >= '{start_ts}' AND date <= '{end_ts}'"

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
            source_metadata=metadata.source_metadata,
            missing_source_paths=metadata.missing_source_paths,
            missing_dates_in_storage=[
                date for date in self._data_model.dates if date not in existing_dates
            ],
        )

    def read(self, **io_kwargs: Any) -> pl.LazyFrame | None:
        lf: pl.LazyFrame | None = None
        if self._is_file_io():
            lf = self._io.read(file_paths=self._file_paths, **io_kwargs)
        elif self._is_table_io():
            lf = self._io.read(table_path=self._table_path, **io_kwargs)
            start_date = self._data_model.start_date
            end_date = self._data_model.end_date
            def _filter_based_on_data_model_date_range(lf: pl.LazyFrame) -> pl.LazyFrame:
                return lf.filter(
                    (pl.col("date").dt.date() >= start_date) & 
                    (pl.col("date").dt.date() <= end_date)
                )
            # Apply date filtering for table IO
            if lf is not None and start_date and end_date:
                if self._data_layer == DataLayer.RAW:
                    # Raw data may not have a proper 'date' column, try best-effort filtering
                    try:
                        if "date" in lf.collect_schema().names():
                            lf = _filter_based_on_data_model_date_range(lf)
                    except Exception:
                        cprint(
                            f"Warning: Could not filter raw data by date range ({start_date} to {end_date}). "
                            f"Returning unfiltered data. Ensure 'date' column exists and is of datetime type.",
                            style=TextStyle.BOLD + RichColor.YELLOW
                        )
                else:
                    lf = _filter_based_on_data_model_date_range(lf)
        elif self._is_database_io():
            lf = self._io.read(db_path=self._db_path, **io_kwargs)
        else:
            raise ValueError(f"Unsupported IO format: {self._io.name}")
        return lf
