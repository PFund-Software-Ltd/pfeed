"""ParquetIO-specific tests, beyond the shared contract in test_io_contract.py."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pfeed.io.base_io import Metadata, Partition

import datetime

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from polars.testing import assert_frame_equal

from pfeed.io.base_io import DatasetKey, DatePartition
from pfeed.io.parquet_io import ParquetIO

NAMESPACE = {'env': 'BACKTEST', 'data_source': 'BYBIT'}
NAME = {'asset_type': 'PERPETUAL', 'resolution': '1t'}
# where ParquetIO puts a dataset with NAMESPACE and NAME, relative to base_path
DATASET_DIR = 'env=BACKTEST/data_source=BYBIT/asset_type=PERPETUAL/resolution=1t'
D1, D2, D3 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)
DATE_KEY = DatasetKey(namespace=NAMESPACE, name=NAME, partition_by=('product', DatePartition('date')))


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


def test_date_partition(tmp_path: Path):
    """A DatePartition level partitions rows by the date of a timestamp column, without storing that date.

    Writes BTC rows on D1 (2 rows) and D3 (1 row), plus D2 as an empty partition, then checks:
    - layout: one year=/month=/day= dir per date, e.g. product=BTC/year=2025/month=01/day=01/part-0.parquet
    - files: only the data's own columns, no year/month/day columns
    - read: the data comes back unchanged and partitions are keyed by date, D2 included
    - replace: rewriting D1 leaves D3 untouched
    """
    io = ParquetIO(base_path=str(tmp_path))
    data = pa.table({
        'date': pa.array([
            datetime.datetime(2025, 1, 1, 1), datetime.datetime(2025, 1, 1, 2), datetime.datetime(2025, 1, 3, 5),
        ], pa.timestamp('ns')),
        'product': ['BTC', 'BTC', 'BTC'],
        'price': [1.0, 2.0, 3.0],
    })
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}, ('BTC', D3): {'version': 1}}

    io.write(DATE_KEY, data, partitions=partitions)

    for date, num_rows in [(D1, 2), (D2, 0), (D3, 1)]:
        file_path = tmp_path / DATASET_DIR / 'product=BTC' / 'year=2025' / 'month=01' / f'day={date.day:02d}' / ParquetIO.FILE_NAME
        file_metadata = pq.read_metadata(file_path)
        assert file_metadata.num_rows == num_rows
        assert file_metadata.schema.names == data.column_names
    lf, read_metadata = io.read(DATE_KEY)
    assert lf is not None
    assert read_metadata == partitions
    assert_frame_equal(lf.collect().sort('date'), pl.DataFrame(data))

    new_d1 = data.slice(0, 1).set_column(2, 'price', pa.array([9.0]))
    io.write(DATE_KEY, new_d1, partitions={('BTC', D1): {'version': 2}})
    lf, read_metadata = io.read(DATE_KEY)
    assert lf is not None
    assert read_metadata == partitions | {('BTC', D1): {'version': 2}}
    assert lf.collect().sort('date')['price'].to_list() == [9.0, 3.0]


def test_date_partition_is_utc_date(tmp_path: Path):
    """A time zone aware timestamp is partitioned by its UTC date, not its local date,
    so every IO (and every time zone) puts a row in the same partition.

    2025-01-01 23:30 UTC is 2025-01-02 07:30 in Hong Kong; its partition is D1.
    """
    io = ParquetIO(base_path=str(tmp_path))
    utc_time = datetime.datetime(2025, 1, 1, 23, 30, tzinfo=datetime.UTC)
    data = pa.table({
        'date': pa.array([utc_time], pa.timestamp('ns', tz='Asia/Hong_Kong')),
        'product': ['BTC'],
    })

    io.write(DATE_KEY, data, partitions={('BTC', D1): {}})

    assert io.read(DATE_KEY)[1] == {('BTC', D1): {}}


def test_date_partition_non_temporal_column_raises(tmp_path: Path):
    """A DatePartition column must be a timestamp/date; e.g. int ns timestamps raise TypeError and write nothing."""
    io = ParquetIO(base_path=str(tmp_path))
    data = pa.table({'date': [1_735_689_600_000_000_000], 'product': ['BTC']})

    with pytest.raises(TypeError):
        io.write(DATE_KEY, data, partitions={('BTC', D1): {}})

    assert io.read(DATE_KEY) == (None, {})


def test_date_partition_dir_name_clash_raises(tmp_path: Path):
    """A column level named like a DatePartition dir (year/month/day) would make two levels share a dir, so it raises."""
    io = ParquetIO(base_path=str(tmp_path))
    key = DatasetKey(namespace=NAMESPACE, name=NAME, partition_by=('year', DatePartition('date')))
    data = pa.table({'date': [D1], 'year': [2025]})

    with pytest.raises(ValueError):
        io.write(key, data, partitions={(2025, D1): {}})
