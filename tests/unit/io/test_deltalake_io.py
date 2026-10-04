"""DeltaLakeIO-specific tests, beyond test_io_contract.py and test_table_io.py."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import datetime
import os

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable

from pfeed.io.base_io import DatasetKey, DatePartition
from pfeed.io.deltalake_io import DeltaLakeIO
from pfeed.io.table_io import VacuumResult

KEY = DatasetKey(
    namespace={'env': 'BACKTEST', 'data_source': 'BYBIT'},
    name={'asset_type': 'PERPETUAL', 'resolution': '1t'},
    partition_by=('product', 'date'),
)
D1, D2, D3 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)


@pytest.fixture
def io(tmp_path: Path) -> DeltaLakeIO:
    return DeltaLakeIO(base_path=str(tmp_path))


@pytest.fixture
def table_path(tmp_path: Path) -> Path:
    """Where KEY's Delta table is stored."""
    return tmp_path / 'BACKTEST__BYBIT' / 'PERPETUAL__1t'


@pytest.fixture
def data() -> pa.Table:
    """3 rows across 3 partitions of KEY: BTC/D1, BTC/D2 and BTC/D3."""
    return pa.table({'ts': [1, 2, 3], 'product': ['BTC'] * 3, 'date': [D1, D2, D3]})


def test_marker_rows(io: DeltaLakeIO, table_path: Path, data: pa.Table):
    """KEY is one Delta table at <base_path>/<schema>/<table>/, with one marker row per partition carrying its metadata,
    so a plain Delta reader sees them too; an empty partition is just its marker row.
    The table is also partitioned by IS_METADATA_COLUMN, so marker rows are in files of their own.
    """
    io.write(KEY, data.slice(0, 1), partitions={('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}})

    dt = DeltaTable(str(table_path))
    raw = pl.read_delta(str(table_path))

    assert dt.metadata().partition_columns == ['product', 'date', io.IS_METADATA_COLUMN]
    markers = raw.filter(pl.col(io.IS_METADATA_COLUMN)).sort('date')
    assert markers.select('product', 'date', io.METADATA_COLUMN).rows() == [
        ('BTC', D1, '{"version": 1}'), ('BTC', D2, '{"version": 1}'),
    ]
    assert markers['ts'].is_null().all()
    data_rows = raw.filter(~pl.col(io.IS_METADATA_COLUMN))
    assert data_rows.select('ts', 'product', 'date', io.METADATA_COLUMN).rows() == [(1, 'BTC', D1, None)]


def test_append_rewrites_no_file(io: DeltaLakeIO, table_path: Path, data: pa.Table):
    """Append is a merge, which only scans the marker file of the partitions it writes to,
    not their data files or the rest of the table, and keeps existing marker rows instead of rewriting them,
    so appending doesn't get slower as a partition grows.

    Writes `data` (3 partitions, each with a data file and a marker file), then appends to BTC/D1:
    1 file (BTC/D1's marker file) scanned, the other 5 skipped, nothing removed, and 1 data file added.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}, ('BTC', D3): {}})

    io.write(KEY, pa.table({'ts': [4], 'product': ['BTC'], 'date': [D1]}), partitions={('BTC', D1): {}}, mode='append')

    (merge,) = DeltaTable(str(table_path)).history(1)
    assert merge['operation'] == 'MERGE'
    assert merge['operationMetrics']['num_target_files_scanned'] == 1
    assert merge['operationMetrics']['num_target_files_skipped_during_scan'] == 5
    assert merge['operationMetrics']['num_target_files_removed'] == 0
    assert merge['operationMetrics']['num_target_files_added'] == 1
    assert merge['operationMetrics']['num_target_rows_copied'] == 0


def test_optimize_compacts_files(io: DeltaLakeIO, table_path: Path, data: pa.Table):
    """optimize() merges a partition's files into one.

    Each partition has a data file and a marker file; BTC/D1 is written by a replace and an append,
    so it has two data files.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}, ('BTC', D3): {}})
    io.write(KEY, pa.table({'ts': [4], 'product': ['BTC'], 'date': [D1]}), partitions={('BTC', D1): {}}, mode='append')
    assert len(DeltaTable(str(table_path)).file_uris()) == 7

    io.optimize()

    assert len(DeltaTable(str(table_path)).file_uris()) == 6


def test_vacuum_deletes_orphans(io: DeltaLakeIO, table_path: Path, data: pa.Table):
    """vacuum() also deletes files the log never referenced (e.g. left by a crashed write), once older than retention."""
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}, ('BTC', D3): {}})
    orphan = table_path / 'orphan.parquet'
    orphan.write_bytes(b'')

    assert io.vacuum(retention=datetime.timedelta(0)) == VacuumResult(deleted_paths=[str(orphan)])  # dry run
    assert orphan.exists()
    assert io.vacuum(retention=datetime.timedelta(0), dry_run=False) == VacuumResult(deleted_paths=[str(orphan)])
    assert not orphan.exists()


def test_vacuum_retention_rounds_up_to_hours(io: DeltaLakeIO, table_path: Path, data: pa.Table):
    """Delta Lake only takes whole hours, so a retention of 1 minute keeps the files of the last hour.

    An orphan modified 30 minutes ago is older than 1 minute, but not than 1 hour.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}, ('BTC', D3): {}})
    orphan = table_path / 'orphan.parquet'
    orphan.write_bytes(b'')
    half_an_hour_ago = datetime.datetime.now().timestamp() - 30 * 60
    os.utime(orphan, (half_an_hour_ago, half_an_hour_ago))

    assert io.vacuum(retention=datetime.timedelta(minutes=1)) == VacuumResult()


def test_date_partition_is_logical(io: DeltaLakeIO, table_path: Path):
    """A DatePartition level isn't a physical partition: the table is partitioned by the column levels
    + IS_METADATA_COLUMN only, so one data file holds both days written together, and replacing D1
    (matched by a date range) leaves D3 in that file untouched.
    """
    key = DatasetKey(namespace=KEY.namespace, name=KEY.name, partition_by=('product', DatePartition('date')))
    data = pa.table({
        'date': pa.array([datetime.datetime(2025, 1, 1, 1), datetime.datetime(2025, 1, 3, 23)], pa.timestamp('us')),
        'product': ['BTC', 'BTC'],
        'price': [1.0, 3.0],
    })
    io.write(key, data, partitions={('BTC', D1): {}, ('BTC', D3): {}})

    assert DeltaTable(str(table_path)).metadata().partition_columns == ['product', io.IS_METADATA_COLUMN]
    assert len([file for file in table_path.rglob('*.parquet') if f'{io.IS_METADATA_COLUMN}=false' in str(file)]) == 1

    io.write(key, data.slice(0, 1).set_column(2, 'price', pa.array([9.0])), partitions={('BTC', D1): {'version': 2}})

    lf, metadata = io.read(key)
    assert lf is not None
    assert lf.sort('date').collect()['price'].to_list() == [9.0, 3.0]
    assert metadata == {('BTC', D1): {'version': 2}, ('BTC', D3): {}}
