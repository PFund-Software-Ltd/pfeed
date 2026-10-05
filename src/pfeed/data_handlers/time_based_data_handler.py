from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime

    import polars as pl

    from pfeed.data_handlers.base_data_handler import BaseDataMetadata
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel
    from pfeed.io.base_io import Partition, PartitionValue

from abc import abstractmethod

from pfeed.data_handlers.base_data_handler import BaseDataHandler
from pfeed.enums import DataLayer
from pfeed.io.base_io import DatasetKey, DatePartition


class TimeBasedDataHandler[DataModelT: TimeBasedDataModel, MetadataT: BaseDataMetadata](
    BaseDataHandler[DataModelT, MetadataT]
):
    """Base for handlers of data with a date column, see TimeBasedDataModel.

    The dataset is partitioned by the subclass's prefix levels, then by day, the day always last:
    the data model is one series over a date range, so reads narrow by series first, then by date.
    """

    @abstractmethod
    def _create_namespace(self) -> dict[str, str]:
        """The DatasetKey's namespace."""

    @abstractmethod
    def _create_name(self) -> dict[str, str]:
        """The DatasetKey's name."""

    @abstractmethod
    def _partition_prefix(self) -> dict[str, PartitionValue]:
        """The partition levels before the date (column -> the data model's value), e.g. {'product': 'BYBIT_BTC_USDT_PERPETUAL'}."""

    def _get_date_col(self) -> str:
        if self._data_layer == DataLayer.RAW:
            return self._data_model.DATE_COL_IN_RAW_DATA
        return self._data_model.DATE_COL_IN_CLEANED_DATA

    def _create_dataset_key(self) -> DatasetKey:
        return DatasetKey(
            namespace=self._create_namespace(),
            name=self._create_name(),
            partition_by=(*self._partition_prefix(), DatePartition(self._get_date_col())),
        )

    def _stream_partitions(self, df: pl.DataFrame) -> dict[Partition, MetadataT]:
        """One partition per day of the date column in `df`, e.g. two if the rows cross midnight."""
        prefix = tuple(self._partition_prefix().values())
        dates = df.get_column(self._get_date_col()).dt.date().unique().sort()
        return {(*prefix, date): self._create_metadata() for date in dates}

    def _partitions(self) -> list[Partition]:
        """The data model's partitions: one per day in its date range."""
        prefix = tuple(self._partition_prefix().values())
        return [(*prefix, date) for date in self._data_model.dates]

    def find_missing_dates_in_storage(self) -> list[datetime.date]:
        """Dates in the data model's range that have no partition in storage."""
        _, metadata = self._read(self._partitions())
        existing_dates = {partition[-1] for partition in metadata}
        return [date for date in self._data_model.dates if date not in existing_dates]
