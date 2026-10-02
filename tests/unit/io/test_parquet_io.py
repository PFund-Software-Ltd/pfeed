"""ParquetIO-specific tests, beyond the shared contract in test_io_contract.py."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pfeed.io.base_io import Metadata, Partition

import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pfeed.io.base_io import DatasetKey
from pfeed.io.parquet_io import ParquetIO

NAMESPACE = {'env': 'BACKTEST', 'data_source': 'BYBIT'}
NAME = {'asset_type': 'PERPETUAL', 'resolution': '1t'}
# where ParquetIO puts a dataset with NAMESPACE and NAME, relative to base_path
DATASET_DIR = 'env=BACKTEST/data_source=BYBIT/asset_type=PERPETUAL/resolution=1t'
D1 = datetime.date(2025, 1, 1)


def test_non_local_base_path_raises():
    """Only local paths are supported for now, so a cloud URI like s3:// raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        ParquetIO(base_path='s3://bucket/data')


@pytest.mark.parametrize('options', [
    {'write_options': {'compression': 'snappy'}},
    {'read_options': {'hive_partitioning': True}},
], ids=['write_options', 'read_options'])
def test_reserved_options_raise(tmp_path: Path, options: dict):
    """write_options/read_options can't set args ParquetIO controls itself, e.g. compression has its own param,
    and hive_partitioning must stay off since partition columns are read from inside the files.
    """
    with pytest.raises(ValueError):
        ParquetIO(base_path=str(tmp_path), **options)


@pytest.mark.parametrize('valid, invalid, error', [
    ('BTC', '', ValueError),  # would be dir 'value=', unreadable
    ('BTC', 'BTC/USDT', ValueError),  # would be read back as two dirs
    (D1, datetime.datetime(2025, 1, 1, 12), TypeError),  # a datetime is a date, but must not become a date dir
], ids=['empty', 'slash', 'datetime'])
def test_invalid_partition_value_raises(tmp_path: Path, valid, invalid, error: type[Exception]):
    """Partition values that can't become a hive dir and be parsed back exactly raise and write nothing.

    Writes a valid partition (with a row) and an invalid one (empty, since a column can't mix e.g. str and datetime).
    The valid partition comes first, so it must not be written before the invalid one raises.
    """
    io = ParquetIO(base_path=str(tmp_path))
    key = DatasetKey(namespace=NAMESPACE, name=NAME, partition_by=('value',))
    data = pa.table({'ts': [1], 'value': [valid]})

    with pytest.raises(error):
        io.write(key, data, partitions={(valid,): {'version': 1}, (invalid,): {'version': 1}})

    assert io.read(key) == (None, {})


def test_int_partition_value(tmp_path: Path):
    """An int partition column is parsed back from its hive dir as int, not left as str.

    Partitions by ('product', 'year') with an int year, then reads the whole dataset,
    which finds the partitions from the dirs only.
    """
    io = ParquetIO(base_path=str(tmp_path))
    key = DatasetKey(namespace=NAMESPACE, name=NAME, partition_by=('product', 'year'))
    data = pa.table({'ts': [1], 'product': ['BTC'], 'year': [2025]})
    partitions: dict[Partition, Metadata] = {('BTC', 2025): {'version': 1}}

    io.write(key, data, partitions=partitions)
    _, read_metadata = io.read(key)

    assert read_metadata == partitions


def test_unsupported_partition_column_type_raises(tmp_path: Path):
    """A partition dir whose column type can't be parsed back (e.g. float) raises TypeError on read.

    ParquetIO never writes such a dir (write rejects float partition values),
    so the file is placed by hand, as if written by something else, with metadata so it counts as committed.
    """
    io = ParquetIO(base_path=str(tmp_path))
    key = DatasetKey(namespace=NAMESPACE, name=NAME, partition_by=('price',))
    partition_dir = tmp_path / DATASET_DIR / 'price=1.5'
    partition_dir.mkdir(parents=True)
    table = pa.table({'ts': [1], 'price': [1.5]}).replace_schema_metadata({ParquetIO.METADATA_KEY: b'{}'})
    pq.write_table(table, partition_dir / ParquetIO.FILE_NAME)

    with pytest.raises(TypeError):
        io.read(key)


def test_file_without_metadata_is_missing(tmp_path: Path):
    """A partition file without pfeed metadata isn't committed (see commit marker), so it is treated as missing.

    Places a part-0.parquet without metadata by hand, e.g. written by something other than ParquetIO.
    Both reading the whole dataset and reading that partition return (None, {}), so it gets downloaded again.
    """
    io = ParquetIO(base_path=str(tmp_path))
    key = DatasetKey(namespace=NAMESPACE, name=NAME, partition_by=('product', 'date'))
    partition_dir = tmp_path / DATASET_DIR / 'product=BTC' / f'date={D1.isoformat()}'
    partition_dir.mkdir(parents=True)
    pq.write_table(pa.table({'ts': [1], 'product': ['BTC'], 'date': [D1]}), partition_dir / ParquetIO.FILE_NAME)

    assert io.read(key) == (None, {})
    assert io.read(key, partitions=[('BTC', D1)]) == (None, {})
