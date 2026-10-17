"""MarketDataHandler tests on a real IO, batch data only (env=BACKTEST); streaming is not covered here.

Only covers what it adds on top of TimeBasedDataHandler (see tests/unit/base/):
the dataset key, the date column per env and layer, and the raw/cleaned write and read paths.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pfund.entities.products.product_base import BaseProduct

import datetime

import pandera.errors
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pfeed.base.time_based_data_model import TimeBasedDataModel
from pfeed.enums import DataLayer
from pfeed.io.base_io import DatasetKey, DatePartition
from pfeed.io.parquet_io import ParquetIO
from pfeed.market.data_handler import MarketDataHandler, MarketDataMetadata
from pfeed.market.data_model import MarketDataModel

D1, D2, D3 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)
RAW_DATE_COL = TimeBasedDataModel.DATE_COL_IN_RAW_DATA


@pytest.fixture
def io(tmp_path: Path) -> ParquetIO:
    return ParquetIO(base_path=str(tmp_path))


def _create_handler(
    io: ParquetIO,
    product: BaseProduct,
    start_date: datetime.date = D1,
    end_date: datetime.date = D3,
    env: str = 'BACKTEST',
    resolution: str = '1d',
    data_layer: DataLayer = DataLayer.CLEANED,
) -> MarketDataHandler:
    data_model = MarketDataModel(
        data_source='BYBIT',
        env=env,
        product=product,
        resolution=resolution,
        start_date=start_date,
        end_date=end_date,
    )
    return MarketDataHandler(data_model, io, data_layer=data_layer)


def _cleaned_df(product: BaseProduct, dates: list[datetime.date]) -> pl.DataFrame:
    """Valid cleaned 1d bars, one per date."""
    n = len(dates)
    return pl.DataFrame({
        'date': pl.Series([datetime.datetime.combine(date, datetime.time()) for date in dates], dtype=pl.Datetime('ns')),
        'product': [product.name] * n,
        'resolution': ['1_DAY'] * n,
        'open': [2.0] * n,
        'high': [3.0] * n,
        'low': [1.0] * n,
        'close': [2.0] * n,
        'volume': [1.0] * n,
    })


def _raw_df(dates: list[datetime.date]) -> pl.DataFrame:
    """Raw data has the vendor's columns plus pfeed's raw date column, one row per date."""
    return pl.DataFrame({
        RAW_DATE_COL: [datetime.datetime.combine(date, datetime.time(12)) for date in dates],
        'vendor_price': [100.0 + i for i in range(len(dates))],
    })


def test_dataset_key(io: ParquetIO, product: BaseProduct):
    handler = _create_handler(io, product)
    assert handler.dataset_key == DatasetKey(
        namespace={
            'env': 'BACKTEST',
            'data_layer': 'CLEANED',
            'data_domain': 'MARKET_DATA',
            'data_source': 'BYBIT',
            'data_origin': 'BYBIT',
        },
        name={'asset_type': str(product.asset_type), 'resolution': '1_DAY'},
        partition_by=('product', DatePartition('date')),
    )


@pytest.mark.parametrize(('env', 'resolution', 'data_layer', 'date_col'), [
    # batch data
    ('BACKTEST', '1d', DataLayer.CLEANED, 'date'),
    ('BACKTEST', '1t', DataLayer.CLEANED, 'date'),
    ('BACKTEST', '1d', DataLayer.RAW, RAW_DATE_COL),
    # streamed data: a bar is in the day it starts
    ('LIVE', '1m', DataLayer.CLEANED, 'start_ts'),
    ('LIVE', '1t', DataLayer.CLEANED, 'ts'),
    ('PAPER', '1m', DataLayer.CLEANED, 'start_ts'),
    ('LIVE', '1m', DataLayer.RAW, RAW_DATE_COL),
])
def test_date_col(io: ParquetIO, product: BaseProduct, env: str, resolution: str, data_layer: DataLayer, date_col: str):
    handler = _create_handler(io, product, env=env, resolution=resolution, data_layer=data_layer)
    assert handler.dataset_key.partition_by == ('product', DatePartition(date_col))


def test_read_nothing_stored(io: ParquetIO, product: BaseProduct):
    assert _create_handler(io, product).read() == (None, {})


def test_write_batch_cleaned_roundtrip(io: ParquetIO, product: BaseProduct):
    handler = _create_handler(io, product)
    df = _cleaned_df(product, [D1, D2, D3])

    handler.write_batch(df)
    lf, metadata = handler.read()

    assert lf is not None
    assert_frame_equal(lf.collect().sort('date'), df, check_column_order=False)
    assert metadata == {(product.name, date): MarketDataMetadata() for date in (D1, D2, D3)}


def test_write_batch_cleaned_validates_schema(io: ParquetIO, product: BaseProduct):
    """Invalid cleaned data raises and nothing is written."""
    handler = _create_handler(io, product)
    df = _cleaned_df(product, [D1, D2, D3]).with_columns(pl.lit(0.5).alias('high'))  # high < low

    with pytest.raises(pandera.errors.SchemaError):
        handler.write_batch(df)
    assert handler.read() == (None, {})


def test_write_batch_stores_days_without_rows(io: ParquetIO, product: BaseProduct):
    """Every day in the date range gets a partition, e.g. a holiday isn't re-downloaded."""
    handler = _create_handler(io, product)
    handler.write_batch(_cleaned_df(product, [D1, D3]))

    _, metadata = handler.read()
    assert set(metadata) == {(product.name, D1), (product.name, D2), (product.name, D3)}
    assert handler.find_missing_dates_in_storage() == []


def test_write_batch_rows_outside_date_range_raise(io: ParquetIO, product: BaseProduct):
    handler = _create_handler(io, product, start_date=D1, end_date=D2)
    with pytest.raises(ValueError, match='not in `partitions`'):
        handler.write_batch(_cleaned_df(product, [D1, D2, D3]))


def test_write_batch_replaces_partitions(io: ParquetIO, product: BaseProduct):
    """Re-writing a date range replaces its rows instead of duplicating them."""
    handler = _create_handler(io, product)
    handler.write_batch(_cleaned_df(product, [D1, D2, D3]))
    df = _cleaned_df(product, [D1, D2, D3]).with_columns(pl.lit(9.0).alias('volume'))

    handler.write_batch(df)
    lf, _ = handler.read()

    assert lf is not None
    assert_frame_equal(lf.collect().sort('date'), df, check_column_order=False)


def test_read_only_data_model_date_range(io: ParquetIO, product: BaseProduct):
    _create_handler(io, product).write_batch(_cleaned_df(product, [D1, D2, D3]))

    lf, metadata = _create_handler(io, product, start_date=D2, end_date=D2).read()

    assert lf is not None
    assert lf.collect().get_column('date').dt.date().to_list() == [D2]
    assert set(metadata) == {(product.name, D2)}


def test_write_batch_raw_roundtrip(io: ParquetIO, product: BaseProduct):
    """Raw data is stored with the product column for partitioning, which read() drops again."""
    handler = _create_handler(io, product, data_layer=DataLayer.RAW)
    df = _raw_df([D1, D2, D3])

    handler.write_batch(df)
    lf, metadata = handler.read()

    assert lf is not None
    assert_frame_equal(lf.collect().sort(RAW_DATE_COL), df, check_column_order=False)
    assert set(metadata) == {(product.name, date) for date in (D1, D2, D3)}
    # the product column is in storage
    stored, _ = io.read(handler.dataset_key)
    assert stored is not None
    assert stored.collect().get_column('product').unique().to_list() == [product.name]


def test_write_batch_raw_is_not_validated(io: ParquetIO, product: BaseProduct):
    """Raw data mirrors the source, it has no schema beyond pfeed's raw date column."""
    handler = _create_handler(io, product, data_layer=DataLayer.RAW)
    df = _raw_df([D1]).with_columns(pl.lit('anything').alias('vendor_col'))

    handler.write_batch(df)
    lf, _ = handler.read()

    assert lf is not None
    assert_frame_equal(lf.collect(), df, check_column_order=False)


def test_write_batch_raw_with_product_column_raises(io: ParquetIO, product: BaseProduct):
    handler = _create_handler(io, product, data_layer=DataLayer.RAW)
    df = _raw_df([D1]).with_columns(pl.lit('x').alias('product'))
    with pytest.raises(ValueError, match="reserved for partitioning"):
        handler.write_batch(df)
