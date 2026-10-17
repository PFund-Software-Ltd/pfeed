"""BaseDataHandler tests, through a minimal concrete handler on a real IO.

Backend behavior is covered by tests/unit/io/test_io_contract.py; these only cover what the handler adds on top,
i.e. normalizing its inputs and (de)serializing the per-partition metadata. Streaming is not covered here.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pfeed.io.base_io import Partition

import datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pfeed.base.data_handler import BaseDataHandler, BaseDataMetadata
from pfeed.base.data_model import BaseDataModel
from pfeed.enums import DataLayer
from pfeed.io.base_io import DatasetKey
from pfeed.io.parquet_io import ParquetIO

KEY = DatasetKey(namespace={'data_source': 'TEST'}, name={'dataset': 'test'}, partition_by=('product',))
D1 = datetime.date(2025, 1, 1)


class _Metadata(BaseDataMetadata):
    # a date isn't JSON-native, so it checks that metadata is dumped to JSON and parsed back to a date
    as_of: datetime.date = D1
    count: int = 0


class _DataModel(BaseDataModel):
    pass


class _Handler(BaseDataHandler[_DataModel, _Metadata]):
    Metadata = _Metadata

    def _create_dataset_key(self) -> DatasetKey:
        return KEY

    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return df

    def write_batch(self, df: pl.DataFrame) -> None:
        self._write(df, {(product,): self._create_metadata() for product in df.get_column('product').unique()})

    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, _Metadata]]:
        return self._read()


@pytest.fixture
def io(tmp_path: Path) -> ParquetIO:
    return ParquetIO(base_path=str(tmp_path))


@pytest.fixture
def handler(io: ParquetIO) -> _Handler:
    return _Handler(_DataModel(data_source='TEST'), io, data_domain='test_domain')


@pytest.fixture
def df() -> pl.DataFrame:
    """3 rows across 2 partitions: BTC (ts 1, 2) and ETH (ts 3)."""
    return pl.DataFrame({'ts': [1, 2, 3], 'price': [100.0, 101.0, 102.0], 'product': ['BTC', 'BTC', 'ETH']})


@pytest.mark.parametrize('data_layer', [DataLayer.RAW, 'raw', 'RAW', 'Raw'])
def test_data_layer_accepts_enum_or_any_case_str(io: ParquetIO, data_layer: DataLayer | str):
    handler = _Handler(_DataModel(data_source='TEST'), io, data_domain='test_domain', data_layer=data_layer)
    assert handler.name.split('.')[2] == 'RAW'


def test_data_layer_defaults_to_cleaned(handler: _Handler):
    assert handler.name.split('.')[2] == 'CLEANED'


def test_name(handler: _Handler):
    """data_domain is upper-cased; the IO is named by its class."""
    assert handler.name == 'TEST.TEST_DOMAIN.CLEANED.ParquetIO'


def test_dataset_key_is_created_by_subclass(handler: _Handler):
    assert handler.dataset_key == KEY


def test_create_metadata_defaults(handler: _Handler):
    assert handler._create_metadata() == _Metadata()


def test_read_missing_dataset(handler: _Handler):
    assert handler.read() == (None, {})


def test_metadata_roundtrip(handler: _Handler, df: pl.DataFrame):
    """Metadata is written as JSON and read back as Metadata instances, with the original types."""
    metadata: dict[Partition, _Metadata] = {('BTC',): _Metadata(as_of=datetime.date(2025, 6, 30), count=2), ('ETH',): _Metadata(count=1)}

    handler._write(df, metadata)
    lf, read_metadata = handler.read()

    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), df, check_column_order=False)
    assert read_metadata == metadata
    assert all(isinstance(md, _Metadata) for md in read_metadata.values())
    assert read_metadata[('BTC',)].as_of == datetime.date(2025, 6, 30)


def test_write_batch_roundtrip(handler: _Handler, df: pl.DataFrame):
    """write_batch() through _create_metadata(), every partition gets the default metadata."""
    handler.write_batch(df)
    _, read_metadata = handler.read()

    assert read_metadata == {('BTC',): _Metadata(), ('ETH',): _Metadata()}


def test_read_partition_subset(handler: _Handler, df: pl.DataFrame):
    handler.write_batch(df)
    lf, read_metadata = handler._read(partitions=[('ETH',)])

    assert lf is not None
    assert lf.collect().get_column('ts').to_list() == [3]
    assert read_metadata == {('ETH',): _Metadata()}


def test_parse_metadata_ignores_unknown_keys(handler: _Handler):
    """Partitions written with a metadata field that no longer exists stay readable."""
    assert handler._parse_metadata({'as_of': '2025-01-02', 'removed_field': 'x'}) == _Metadata(
        as_of=datetime.date(2025, 1, 2)
    )


def test_parse_metadata_fills_missing_keys_with_defaults(handler: _Handler):
    """Partitions written before a metadata field was added stay readable."""
    assert handler._parse_metadata({}) == _Metadata()
