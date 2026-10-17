"""TimeBasedDataHandler tests, through a minimal concrete handler on a real IO.

Only covers what it adds on top of BaseDataHandler (see test_data_handler.py):
the dataset key's partition levels, the data model's partitions and finding the missing dates.
Streaming is not covered here.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pfeed.io.base_io import Partition, PartitionValue

import datetime

import polars as pl
import pytest

from pfeed.base.data_handler import BaseDataMetadata
from pfeed.base.time_based_data_handler import TimeBasedDataHandler
from pfeed.base.time_based_data_model import TimeBasedDataModel
from pfeed.enums import DataLayer
from pfeed.io.base_io import DatasetKey, DatePartition
from pfeed.io.parquet_io import ParquetIO

D1, D2, D3 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)


class _DataModel(TimeBasedDataModel):
    product: str = 'BTC'


class _Handler(TimeBasedDataHandler[_DataModel, BaseDataMetadata]):
    Metadata = BaseDataMetadata

    def _create_namespace(self) -> dict[str, str]:
        return {'data_source': self._data_model.data_source, 'data_layer': str(self._data_layer)}

    def _create_name(self) -> dict[str, str]:
        return {'dataset': 'test'}

    def _partition_prefix(self) -> dict[str, PartitionValue]:
        return {'product': self._data_model.product}

    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return df

    def write_batch(self, df: pl.DataFrame) -> None:
        # like MarketDataHandler, every day in the date range gets a partition, even without rows
        self._write(df, {partition: self._create_metadata() for partition in self._partitions()})

    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, BaseDataMetadata]]:
        return self._read(self._partitions())


@pytest.fixture
def io(tmp_path: Path) -> ParquetIO:
    return ParquetIO(base_path=str(tmp_path))


def _create_handler(
    io: ParquetIO,
    start_date: datetime.date = D1,
    end_date: datetime.date = D3,
    product: str = 'BTC',
    data_layer: DataLayer = DataLayer.CLEANED,
) -> _Handler:
    data_model = _DataModel(data_source='TEST', start_date=start_date, end_date=end_date, product=product)
    return _Handler(data_model, io, data_domain='test_domain', data_layer=data_layer)


def _df(dates: list[datetime.date], product: str = 'BTC', date_col: str = 'date') -> pl.DataFrame:
    """One row per date, at 12:00 of that day."""
    return pl.DataFrame({
        date_col: [datetime.datetime.combine(date, datetime.time(12)) for date in dates],
        'price': [100.0 + i for i in range(len(dates))],
        'product': [product] * len(dates),
    })


@pytest.mark.parametrize(('data_layer', 'date_col'), [
    (DataLayer.RAW, TimeBasedDataModel.DATE_COL_IN_RAW_DATA),
    (DataLayer.CLEANED, 'date'),
    (DataLayer.CURATED, 'date'),
])
def test_dataset_key(io: ParquetIO, data_layer: DataLayer, date_col: str):
    """Partitioned by the prefix levels, then by the day of the layer's date column, the day always last."""
    handler = _create_handler(io, data_layer=data_layer)
    assert handler.dataset_key == DatasetKey(
        namespace={'data_source': 'TEST', 'data_layer': str(data_layer)},
        name={'dataset': 'test'},
        partition_by=('product', DatePartition(date_col)),
    )


def test_partitions(io: ParquetIO):
    """One partition per day in the date range, both ends included."""
    handler = _create_handler(io)
    assert handler._partitions() == [('BTC', D1), ('BTC', D2), ('BTC', D3)]


def test_partitions_single_date(io: ParquetIO):
    handler = _create_handler(io, start_date=D2, end_date=D2)
    assert handler._partitions() == [('BTC', D2)]


def test_find_missing_dates_nothing_stored(io: ParquetIO):
    assert _create_handler(io).find_missing_dates_in_storage() == [D1, D2, D3]


def test_find_missing_dates_partially_stored(io: ParquetIO):
    _create_handler(io, start_date=D2, end_date=D2).write_batch(_df([D2]))
    assert _create_handler(io).find_missing_dates_in_storage() == [D1, D3]


def test_find_missing_dates_all_stored(io: ParquetIO):
    handler = _create_handler(io)
    handler.write_batch(_df([D1, D2, D3]))
    assert handler.find_missing_dates_in_storage() == []


def test_find_missing_dates_empty_partition_is_stored(io: ParquetIO):
    """A day without rows (e.g. a holiday) is stored as an empty partition, so it isn't missing."""
    handler = _create_handler(io)
    handler.write_batch(_df([D1, D3]))
    assert handler.find_missing_dates_in_storage() == []


def test_find_missing_dates_ignores_other_prefix(io: ParquetIO):
    """Another product's partitions on the same days don't count."""
    _create_handler(io, product='ETH').write_batch(_df([D1, D2, D3], product='ETH'))
    assert _create_handler(io).find_missing_dates_in_storage() == [D1, D2, D3]


def test_find_missing_dates_raw_layer(io: ParquetIO):
    """Raw data is partitioned by its own date column, not by 'date'."""
    handler = _create_handler(io, start_date=D1, end_date=D2, data_layer=DataLayer.RAW)
    handler.write_batch(_df([D1, D2], date_col=TimeBasedDataModel.DATE_COL_IN_RAW_DATA))
    assert handler.find_missing_dates_in_storage() == []
