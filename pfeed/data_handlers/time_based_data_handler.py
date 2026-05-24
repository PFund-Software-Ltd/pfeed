# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false
from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, cast, assert_never

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame
    from pfeed._io.database_io import DatabaseIO
    from pfeed.storages.database_storage import DatabaseURI
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel
    from pfeed._io.base_io import BaseIO
    from pfeed._sinks.base_sink import BaseSink
    from pfeed.data_handlers.base_data_handler import SourcePath, IOClassName

import datetime
from abc import ABC

import polars as pl
import pyarrow as pa

from pfeed.utils.file_path import FilePath
from pfeed._io.table_io import TablePath
from pfeed._io.database_io import DBPath
from pfeed.enums import DataLayer, DataTool, IOType
from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseDataMetadata


class TimeBasedDataMetadata(BaseDataMetadata):
    dates: list[datetime.date]


class TimeBasedDataHandler(BaseDataHandler, ABC):
    PARTITION_COLUMNS: ClassVar[list[str]] = ["year", "month", "day"]  # used by e.g. Delta Lake for partitioning
    # NOTE: using class names instead of the actual IO classes to avoid import error (e.g. deltalake not installed)
    IO_USING_PARTITION_COLUMNS: ClassVar[set[IOClassName]] = {'DeltaLakeIO'}

    _data_model: TimeBasedDataModel
    metadata_class: ClassVar[type[TimeBasedDataMetadata]] = TimeBasedDataMetadata

    def __init__(
        self,
        data_path: FilePath | DatabaseURI,
        data_layer: DataLayer,
        data_domain: str,
        data_model: TimeBasedDataModel,
        io: BaseIO,
        sink: BaseSink | None = None,
    ):
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            data_model=data_model,
            io=io,
            sink=sink,
        )
        if self._io:
            self._file_paths_per_date = {
                date: self._create_file_path(date=date) for date in data_model.dates
            } if self._io.is_file_io() else {}
            self._file_paths: list[FilePath] = list(self._file_paths_per_date.values())
        if self._sink and self._requires_partitioning():
            self._sink.set_partitioning(
                partition_func=self._partition_stream_data,
                partition_columns=self.PARTITION_COLUMNS,
            )

    def _create_file_path(self, date: datetime.date) -> FilePath:
        raise NotImplementedError(f'{self.__class__.__name__} is not implemented')

    def _create_table_path(self) -> TablePath:
        raise NotImplementedError(f'{self.__class__.__name__} is not implemented')

    def _create_db_path(self) -> DBPath:
        raise NotImplementedError(f'{self.__class__.__name__} is not implemented')

    def _create_metadata(self, dates: list[datetime.date]) -> TimeBasedDataMetadata:
        raise NotImplementedError(f'{self.__class__.__name__} is not implemented')

    @staticmethod
    def _partition_batch_data(date_col: str, df: pl.LazyFrame) -> pl.LazyFrame:
        # Preprocess table to add year, month, day columns for partitioning
        return df.with_columns(
            pl.col(date_col).dt.year().alias("year"),
            pl.col(date_col).dt.month().alias("month"),
            pl.col(date_col).dt.day().alias("day"),
        )

    @staticmethod
    def _partition_stream_data(table: pa.Table) -> pa.Table:
        """Partition the streaming data by year, month, and day.

        Args:
            table: pyarrow table (converted by streaming buffer in sink) to partition.

        Returns:
            The partitioned table.
        """
        import pyarrow.compute as pc
        ts = pc.cast(table["ts"], pa.timestamp("ns", tz="UTC"))
        return (
            table
            .append_column("year",  pc.year(ts))
            .append_column("month", pc.month(ts))
            .append_column("day",   pc.day(ts))
        )

    def write_batch(self, data: IntoFrame):
        with self.io:
            from pfeed._etl.base import convert_dataframe
            df = cast(pl.LazyFrame, convert_dataframe(data, DataTool.polars))
            self._write_batch(df)

    def find_missing_dates_in_storage(self):
        existing_metadata_dict = cast("dict[SourcePath, TimeBasedDataMetadata]", self.read_metadata())
        existing_dates = []
        match self._io_type:
            case IOType.FILE:
                existing_dates = [
                    date
                    for metadata in existing_metadata_dict.values()
                    for date in metadata.dates
                ]
            case (IOType.TABLE | IOType.DATABASE) as io_type:
                source_path = self._table_path if io_type == IOType.TABLE else self._db_path
                assert source_path is not None, f'source_path is not set for {self.io.name}'
                existing_metadata = existing_metadata_dict.get(source_path, None)
                if existing_metadata is not None:
                    existing_dates = existing_metadata.dates
            case _:
                assert_never(self._io_type)
        return [date for date in self._data_model.dates if date not in existing_dates]

    def _get_date_col(self) -> str:
        from pfeed.feeds.time_based_feed import TimeBasedFeed
        return TimeBasedFeed.DATE_COL_IN_CLEANED_DATA if self._data_layer != DataLayer.RAW else TimeBasedFeed.DATE_COL_IN_RAW_DATA

    def _write_batch(self, lf: pl.LazyFrame):
        is_raw_data = self._data_layer == DataLayer.RAW
        date_col = self._get_date_col()

        # validate before writing data
        if not is_raw_data:
            lf = self._validate_schema(lf)

        df = lf.collect()
        data_model: TimeBasedDataModel = self._data_model
        write_kwargs = {}

        match self._io_type:
            case IOType.FILE:
                # split data with a date range into chunks per date
                if df.is_empty():
                    data_chunks_per_date = {}
                else:
                    data_chunks_per_date = {
                        group_key[0]: group
                        for group_key, group in df.group_by(pl.col(date_col).dt.date())
                    }
                # split data model spanning multiple dates into separate data models, each with a single date
                for date in data_model.dates:
                    # NOTE: create placeholder data if date is not in data_chunks_per_date,
                    # used as an indicator for successful download, there is just no data on that date (e.g. weekends, holidays, etc.)
                    df_chunk = data_chunks_per_date.get(date, df.clear())
                    file_path = self._file_paths_per_date[date]
                    table = df_chunk.to_arrow()
                    metadata = self._create_metadata(dates=[date])
                    # NOTE: this only writes metadata to the table schema, not to the file.
                    table_with_metadata = self.io.write_metadata(table, metadata)
                    self.io.write(data=table_with_metadata, file_path=file_path)
            case (IOType.TABLE | IOType.DATABASE) as io_type:
                if io_type == IOType.TABLE:
                    source_path = self._table_path
                    if self._requires_partitioning():
                        df = self._partition_batch_data(date_col, df)
                        write_kwargs["partition_by"] = self.PARTITION_COLUMNS
                elif io_type == IOType.DATABASE:
                    source_path = self._db_path
                    if df.is_empty():
                        import warnings
                        warnings.warn(
                            f"Empty DataFrame; skipping database write for db_path={source_path}",
                            RuntimeWarning,
                            stacklevel=2,
                        )
                        return
                    # IO owns the precision policy (e.g. DuckDB stores at microsecond).
                    # conform() casts datetime columns to the IO's TIMESTAMP_PRECISION so
                    # the start/end ts we derive below match what will actually be stored.
                    io = cast("DatabaseIO", self.io)
                    df = cast(pl.DataFrame, io.conform(df))
                else:
                    raise ValueError(f'Unsupported IO format: {self.io.name}')

                # Replace any overlapping data within the date range
                if not df.is_empty():
                    start_date = df[date_col].min()
                    end_date = df[date_col].max()
                    if not self.io.DATE_FILTER_PREDICATE:
                        raise ValueError(f"IO {self.io.name} has no DATE_FILTER_PREDICATE")
                    delete_where = self.io.DATE_FILTER_PREDICATE.format(date_col=date_col, start_date=start_date, end_date=end_date)
                else:
                    delete_where = None

                self.io.write(
                    df.to_arrow(),
                    source_path,
                    delete_where=delete_where,
                    **write_kwargs,
                )
                self.io.write_metadata(source_path, metadata=self._create_metadata(dates=data_model.dates))
            case _:
                assert_never(self._io_type)

    def read(self) -> pl.LazyFrame | None:
        date_col = self._get_date_col()
        lf: pl.LazyFrame | None = None
        match self._io_type:
            case IOType.FILE:
                lf = self.io.read(file_paths=self._file_paths)
            case (IOType.TABLE | IOType.DATABASE) as io_type:
                source_path = self._table_path if io_type == IOType.TABLE else self._db_path
                lf: pl.LazyFrame | None = self.io.read(source_path)
                start_date, end_date = self._data_model.start_date, self._data_model.end_date
                if lf is not None:
                    lf = lf.filter(
                        (pl.col(date_col).dt.date() >= start_date) &
                        (pl.col(date_col).dt.date() <= end_date)
                    )
            case _:
                assert_never(self._io_type)
        return lf
