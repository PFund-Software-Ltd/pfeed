"""Tests every TableIO (DuckLake, Delta Lake, Iceberg) must pass, beyond the shared contract in test_io_contract.py."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from pfeed.io.base_io import Metadata, Partition

import datetime
from functools import partial

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from polars.testing import assert_frame_equal

from pfeed.io.base_io import DatasetKey
from pfeed.io.deltalake_io import DeltaLakeIO
from pfeed.io.ducklake_io import DuckLakeIO
from pfeed.io.iceberg_io import IcebergIO
from pfeed.io.table_io import TableIO, VacuumResult

TABLE_IOS = [DuckLakeIO, DeltaLakeIO, IcebergIO]
# the test data is small enough to be inlined into DuckLake's catalog, so also test with every write as parquet files
FILE_WRITING_IOS = [partial(DuckLakeIO, data_inlining_row_limit=0), DeltaLakeIO, IcebergIO]


@pytest.fixture(params=TABLE_IOS, ids=[io_class.__name__ for io_class in TABLE_IOS])
def io(request, tmp_path: Path) -> TableIO:
    return request.param(base_path=str(tmp_path))


@pytest.fixture(params=FILE_WRITING_IOS, ids=['DuckLakeIO-no_inlining', 'DeltaLakeIO', 'IcebergIO'])
def file_io(request, tmp_path: Path) -> TableIO:
    """Every write creates parquet files, so maintenance has files to compact/delete."""
    return request.param(base_path=str(tmp_path))


KEY = DatasetKey(
    namespace={'env': 'BACKTEST', 'data_source': 'BYBIT'},
    name={'asset_type': 'PERPETUAL', 'resolution': '1t'},
    partition_by=('product', 'date'),
)
D1, D2 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2)


@pytest.fixture
def data() -> pa.Table:
    """3 rows across 2 partitions of KEY: BTC/D1 (ts 1, 2) and BTC/D2 (ts 3)."""
    return pa.table({
        'ts': [1, 2, 3],
        'price': [100.0, 101.0, 102.0],
        'product': ['BTC', 'BTC', 'BTC'],
        'date': [D1, D1, D2],
    })


def _parquet_files(path: Path) -> set[str]:
    return {str(file_path) for file_path in path.rglob('*.parquet')}


@pytest.mark.parametrize('io_class', TABLE_IOS, ids=[io_class.__name__ for io_class in TABLE_IOS])
def test_non_local_base_path_raises(io_class: type[TableIO]):
    """Only local paths are supported for now, so a cloud URI like s3:// raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        io_class(base_path='s3://bucket/data')


def test_read_is_pinned_to_version(io: TableIO, data: pa.Table):
    """The LazyFrame from read() keeps reading the table version its metadata came from,
    even if the dataset is replaced before it is collected, so data and metadata always match.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)
    lf, _ = io.read(KEY)

    io.write(KEY, data.schema.empty_table(), partitions={('BTC', D1): {'version': 2}})

    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


@pytest.mark.parametrize(
    'value',
    ['', 'BYBIT__SPOT', '_BYBIT', 'BYBIT_', 'BY/BIT', '.BYBIT'],
    ids=['empty', 'separator', 'leading', 'trailing', 'slash', 'dot'],
)
def test_ambiguous_key_value_raises(io: TableIO, data: pa.Table, value: str):
    """Key values are joined by NAME_SEPARATOR into schema/table names, so a value that could make
    two different keys join to the same name raises ValueError, e.g. ('A_', 'B') and ('A', '_B') -> 'A___B'.
    So does a value that can't be a directory name, since some formats store each table in <schema>/<table>/.
    """
    key = DatasetKey(namespace=KEY.namespace | {'data_source': value}, name=KEY.name, partition_by=KEY.partition_by)

    with pytest.raises(ValueError):
        io.write(key, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    with pytest.raises(ValueError):
        io.read(key)


@pytest.mark.parametrize('other_key', [
    DatasetKey(namespace=KEY.namespace, name=KEY.name | {'resolution': '1T'}, partition_by=KEY.partition_by),
    DatasetKey(namespace=KEY.namespace | {'env': 'backtest'}, name=KEY.name, partition_by=KEY.partition_by),
], ids=['table', 'schema'])
def test_case_only_difference_raises(io: TableIO, data: pa.Table, other_key: DatasetKey):
    """Names are case-insensitive somewhere along the way (DuckDB names, macOS paths), so a key differing
    from an existing dataset only by case could silently read/write that dataset; it raises ValueError instead.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    with pytest.raises(ValueError):
        io.write(other_key, data, partitions=partitions)
    with pytest.raises(ValueError):
        io.read(other_key)


@pytest.mark.parametrize('column', [TableIO.METADATA_COLUMN, TableIO.IS_METADATA_COLUMN])
def test_reserved_metadata_column_raises(io: TableIO, data: pa.Table, column: str):
    """data can't have the columns METADATA_COLUMN and IS_METADATA_COLUMN, they are reserved for pfeed's metadata;
    nothing is written.
    """
    data = data.append_column(column, pa.array([None, None, None]))

    with pytest.raises(ValueError):
        io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})

    assert io.read(KEY) == (None, {})


@pytest.mark.parametrize('invalid', [datetime.datetime(2025, 1, 2, 12), '2025-01-02'], ids=['datetime', 'str'])
def test_partition_value_type_mismatch_raises(io: TableIO, invalid):
    """A partition value whose type doesn't match data's partition column raises TypeError and writes nothing.

    The valid BTC/D1 partition (with a row) must not be written either.
    A datetime is a date, but would be silently truncated to one, so it is rejected too.
    """
    data = pa.table({'ts': [1], 'product': ['BTC'], 'date': [D1]})

    with pytest.raises(TypeError):
        io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', invalid): {}})

    assert io.read(KEY) == (None, {})


def test_retry_recovers(io: TableIO, data: pa.Table, mocker: MockerFixture):
    """A write that conflicted with a concurrent one is retried as a whole, and then lands."""
    mocker.patch.object(type(io), 'RETRY_WAIT', 0)
    write_transaction = type(io)._write_transaction  # ty: ignore[unresolved-attribute]
    calls = []

    def conflicting_once(self, *args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise self._RETRY_ON[0]('conflict')
        return write_transaction(self, *args, **kwargs)

    mocker.patch.object(type(io), '_write_transaction', conflicting_once)
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)
    lf, read_metadata = io.read(KEY)

    assert len(calls) == 2
    assert lf is not None
    assert read_metadata == partitions
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_retry_gives_up(io: TableIO, data: pa.Table, mocker: MockerFixture):
    """A write that keeps conflicting is retried MAX_RETRIES times, then raises the conflict."""
    mocker.patch.object(type(io), 'MAX_RETRIES', 2)
    mocker.patch.object(type(io), 'RETRY_WAIT', 0)
    conflict = io._RETRY_ON[0]
    write_transaction = mocker.patch.object(type(io), '_write_transaction', side_effect=conflict('conflict'))

    with pytest.raises(conflict, match='gave up after 2 retries'):
        io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})

    assert write_transaction.call_count == 3


def test_optimize_keeps_data(file_io: TableIO, data: pa.Table):
    """optimize() rewrites files, but reads return the same data and metadata.

    BTC/D1 is written by a replace and an append, so it has more than one file to compact.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    file_io.write(KEY, data, partitions=partitions)
    new_data = pa.table({'ts': [4], 'price': [103.0], 'product': ['BTC'], 'date': [D1]})
    file_io.write(KEY, new_data, partitions={('BTC', D1): {'version': 2}}, mode='append')

    file_io.optimize()
    lf, read_metadata = file_io.read(KEY)

    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 2}, ('BTC', D2): {'version': 1}}
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(pa.concat_tables([data, new_data])))


def test_optimize_empty_lake(io: TableIO):
    """optimize() and vacuum() on a lake without tables do nothing."""
    io.optimize()
    assert io.vacuum(retention=datetime.timedelta(0), dry_run=False) == VacuumResult()


def test_vacuum(file_io: TableIO, tmp_path: Path, data: pa.Table):
    """vacuum() deletes the files only old versions use, once they are older than `retention`.

    Writes `data`, then replaces BTC/D1, so its first file is only used by an old version.
    - default retention (7 days): nothing is deleted
    - dry run: returns what would be deleted, but deletes nothing
    - retention=0: the replaced file is deleted (DuckLake takes two vacuums: one expires, the next deletes)
    Reads are unchanged after each.
    """
    file_io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    new_data = pa.table({'ts': [4], 'price': [103.0], 'product': ['BTC'], 'date': [D1]})
    file_io.write(KEY, new_data, partitions={('BTC', D1): {}})
    expected = pl.DataFrame(pa.concat_tables([data.filter(pc.field('date') == D2), new_data])).sort('ts')
    files_before = _parquet_files(tmp_path)

    assert file_io.vacuum(dry_run=False) == VacuumResult()
    file_io.vacuum(retention=datetime.timedelta(0))  # dry run
    assert _parquet_files(tmp_path) == files_before

    deleted = file_io.vacuum(retention=datetime.timedelta(0), dry_run=False).deleted_paths
    deleted += file_io.vacuum(retention=datetime.timedelta(0), dry_run=False).deleted_paths
    lf, _ = file_io.read(KEY)

    deleted_parquet_files = {path for path in deleted if path.endswith('.parquet')}
    assert deleted_parquet_files  # the replaced BTC/D1 file
    assert _parquet_files(tmp_path) == files_before - deleted_parquet_files
    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), expected)


def test_negative_retention_raises(io: TableIO):
    with pytest.raises(ValueError):
        io.vacuum(retention=datetime.timedelta(hours=-1))
