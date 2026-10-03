"""DuckLakeIO-specific tests, beyond the shared contract in test_io_contract.py."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from pfeed.io.base_io import Metadata, Partition

import datetime
import multiprocessing
from functools import partial

import duckdb
import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

from pfeed.io.base_io import DatasetKey
from pfeed.io.ducklake_io import DuckLakeIO

KEY = DatasetKey(
    namespace={'env': 'BACKTEST', 'data_source': 'BYBIT'},
    name={'asset_type': 'PERPETUAL', 'resolution': '1t'},
    partition_by=('product', 'date'),
)
D1, D2 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2)


@pytest.fixture
def io(tmp_path: Path) -> DuckLakeIO:
    return DuckLakeIO(base_path=str(tmp_path))


@pytest.fixture
def data() -> pa.Table:
    """3 rows across 2 partitions of KEY: BTC/D1 (ts 1, 2) and BTC/D2 (ts 3)."""
    return pa.table({
        'ts': [1, 2, 3],
        'price': [100.0, 101.0, 102.0],
        'product': ['BTC', 'BTC', 'BTC'],
        'date': [D1, D1, D2],
    })


def test_non_local_base_path_raises():
    """Only local paths are supported for now, so a cloud URI like s3:// raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        DuckLakeIO(base_path='s3://bucket/data')


@pytest.mark.parametrize('limit', [-1, 1.5], ids=['negative', 'float'])
def test_invalid_data_inlining_row_limit_raises(tmp_path: Path, limit):
    """data_inlining_row_limit goes into the ATTACH statement, so anything but an int >= 0 raises ValueError."""
    with pytest.raises(ValueError):
        DuckLakeIO(base_path=str(tmp_path), data_inlining_row_limit=limit)


@pytest.mark.parametrize('limit, inlined', [(None, True), (0, False)], ids=['default', 'disabled'])
def test_data_inlining(tmp_path: Path, data: pa.Table, limit: int | None, inlined: bool):
    """A small write is stored inside the catalog by default, but as a parquet file when inlining is disabled.

    Counts the parquet files of the data table, not the metadata table, after writing `data` (3 rows).
    """
    io = DuckLakeIO(base_path=str(tmp_path), data_inlining_row_limit=limit)
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})

    data_dir = tmp_path / DuckLakeIO.DATA_DIR_NAME / 'BACKTEST__BYBIT' / 'PERPETUAL__1t'
    assert (not any(data_dir.rglob('*.parquet'))) == inlined


def test_append(io: DuckLakeIO, data: pa.Table):
    """Append adds rows to the given partitions and replaces their metadata; other partitions are untouched.

    Writes `data` (BTC/D1 and BTC/D2), then appends a row to BTC/D1 with new metadata.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    new_data = pa.table({'ts': [4], 'price': [103.0], 'product': ['BTC'], 'date': [D1]})
    io.write(KEY, new_data, partitions={('BTC', D1): {'version': 2}}, mode='append')
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 2}, ('BTC', D2): {'version': 1}}
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(pa.concat_tables([data, new_data])))


def test_read_is_pinned_to_snapshot(io: DuckLakeIO, data: pa.Table):
    """The LazyFrame from read() keeps reading the snapshot its metadata came from,
    even if the dataset is replaced before it is collected, so data and metadata always match.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)
    lf, _ = io.read(KEY)

    io.write(KEY, data.schema.empty_table(), partitions={('BTC', D1): {'version': 2}})

    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_reserved_metadata_suffix_raises(io: DuckLakeIO, data: pa.Table):
    """A dataset name ending with the metadata table suffix would clash with another dataset's metadata table."""
    key = DatasetKey(namespace=KEY.namespace, name=KEY.name | {'kind': 'metadata'}, partition_by=KEY.partition_by)

    with pytest.raises(ValueError):
        io.write(key, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    with pytest.raises(ValueError):
        io.read(key)


@pytest.mark.parametrize('value', ['', 'BYBIT__SPOT', '_BYBIT', 'BYBIT_'], ids=['empty', 'separator', 'leading', 'trailing'])
def test_ambiguous_key_value_raises(io: DuckLakeIO, data: pa.Table, value: str):
    """Key values are joined by NAME_SEPARATOR into schema/table names, so a value that could make
    two different keys join to the same name raises ValueError, e.g. ('A_', 'B') and ('A', '_B') -> 'A___B'.
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
def test_case_only_difference_raises(io: DuckLakeIO, data: pa.Table, other_key: DatasetKey):
    """DuckDB names are case-insensitive, so a key differing from an existing dataset only by case
    would silently read/write that dataset; it raises ValueError instead.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    with pytest.raises(ValueError):
        io.write(other_key, data, partitions=partitions)
    with pytest.raises(ValueError):
        io.read(other_key)


@pytest.mark.parametrize('invalid', [datetime.datetime(2025, 1, 2, 12), '2025-01-02'], ids=['datetime', 'str'])
def test_partition_value_type_mismatch_raises(io: DuckLakeIO, invalid):
    """A partition value whose type doesn't match data's partition column raises TypeError and writes nothing.

    The valid BTC/D1 partition (with a row) must not be written either.
    A datetime is a date, but would be silently truncated to one, so it is rejected too.
    """
    data = pa.table({'ts': [1], 'product': ['BTC'], 'date': [D1]})

    with pytest.raises(TypeError):
        io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', invalid): {}})

    assert io.read(KEY) == (None, {})


def test_retry_gives_up(io: DuckLakeIO, data: pa.Table, mocker: MockerFixture):
    """A write that keeps conflicting is retried MAX_RETRIES times, then raises TransactionException."""
    mocker.patch.object(DuckLakeIO, 'MAX_RETRIES', 2)
    mocker.patch.object(DuckLakeIO, 'RETRY_WAIT', 0)
    write_transaction = mocker.patch.object(
        DuckLakeIO, '_write_transaction', side_effect=duckdb.TransactionException('conflict'),
    )

    with pytest.raises(duckdb.TransactionException, match='gave up after 2 retries'):
        io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})

    assert write_transaction.call_count == 3


def test_concurrent_partition_writes(io: DuckLakeIO):
    """Processes writing disjoint partitions of the same dataset at the same time all land.

    DuckLake detects conflicts per table, so these writes do conflict and must be retried.
    A pool of 4 processes writes 20 partitions, one write each, with partition i having i + 1 rows.
    The io is pickled into each process (spawn), like a Ray worker.
    """
    products = [f'P{i}' for i in range(20)]
    partitions: list[dict[Partition, Metadata]] = [{(product, D1): {'rows': i + 1}} for i, product in enumerate(products)]
    writes = [
        partial(
            io.write,
            KEY,
            pa.table({'ts': list(range(i + 1)), 'product': [product] * (i + 1), 'date': [D1] * (i + 1)}),
            partitions=partitions[i],
        )
        for i, product in enumerate(products)
    ]
    with multiprocessing.get_context('spawn').Pool(4) as pool:
        for result in [pool.apply_async(write) for write in writes]:
            result.get()

    lf, read_metadata = io.read(KEY)
    assert lf is not None
    assert read_metadata == {(product, D1): {'rows': i + 1} for i, product in enumerate(products)}
    counts = lf.group_by('product').len().collect()
    assert dict(counts.iter_rows()) == {product: i + 1 for i, product in enumerate(products)}


def test_optimize_moves_inlined_data_to_files(io: DuckLakeIO, tmp_path: Path, data: pa.Table):
    """optimize() moves the inlined rows of a small write out of the catalog into parquet files; reads are unchanged."""
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    data_dir = tmp_path / DuckLakeIO.DATA_DIR_NAME / 'BACKTEST__BYBIT' / 'PERPETUAL__1t'
    assert not any(data_dir.rglob('*.parquet'))

    io.optimize()
    lf, _ = io.read(KEY)

    assert any(data_dir.rglob('*.parquet'))
    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_vacuum(tmp_path: Path, data: pa.Table):
    """vacuum() deletes files that old snapshots used, but only a retention period after expiring them.

    Writes `data`, then replaces BTC/D1, so its first file is only used by the expired snapshot.
    A stray parquet file (like one left by a crashed write) is deleted as an orphan.
    retention=0, so the second vacuum deletes the files the first one expired the snapshots of.
    """
    io = DuckLakeIO(base_path=str(tmp_path), data_inlining_row_limit=0)
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    new_data = pa.table({'ts': [4], 'price': [103.0], 'product': ['BTC'], 'date': [D1]})
    io.write(KEY, new_data, partitions={('BTC', D1): {}})
    orphan = tmp_path / DuckLakeIO.DATA_DIR_NAME / 'orphan.parquet'
    orphan.write_bytes(b'')
    files_before = {str(path) for path in tmp_path.rglob('*.parquet')}

    snapshot_ids, paths = io.vacuum(retention=datetime.timedelta(0))  # dry run
    assert snapshot_ids
    assert paths == [str(orphan)]
    assert {str(path) for path in tmp_path.rglob('*.parquet')} == files_before

    _, first_paths = io.vacuum(retention=datetime.timedelta(0), dry_run=False)
    _, second_paths = io.vacuum(retention=datetime.timedelta(0), dry_run=False)
    lf, _ = io.read(KEY)

    assert first_paths == [str(orphan)]
    assert second_paths  # the replaced BTC/D1 file
    assert {str(path) for path in tmp_path.rglob('*.parquet')} == files_before - {*first_paths, *second_paths}
    assert lf is not None
    expected = pa.concat_tables([new_data, data.filter(pa.compute.equal(data['date'], D2))])
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(expected).sort('ts'))


def test_negative_retention_raises(io: DuckLakeIO):
    with pytest.raises(ValueError):
        io.vacuum(retention=datetime.timedelta(hours=-1))
