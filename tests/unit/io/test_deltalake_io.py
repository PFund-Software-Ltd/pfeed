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

from pfeed.io.base_io import DatasetKey
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


def test_append_only_rewrites_marker_file(io: DeltaLakeIO, table_path: Path, data: pa.Table):
    """Append is a merge, which only scans and rewrites the marker file of the partitions it writes to,
    not their data files or the rest of the table, so appending doesn't get slower as a partition grows.

    Writes `data` (3 partitions, each with a data file and a marker file), then appends to BTC/D1:
    1 file (BTC/D1's marker file) scanned and replaced, the other 5 skipped, and 1 data file added.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}, ('BTC', D3): {}})

    io.write(KEY, pa.table({'ts': [4], 'product': ['BTC'], 'date': [D1]}), partitions={('BTC', D1): {}}, mode='append')

    (merge,) = DeltaTable(str(table_path)).history(1)
    assert merge['operation'] == 'MERGE'
    assert merge['operationMetrics']['num_target_files_scanned'] == 1
    assert merge['operationMetrics']['num_target_files_skipped_during_scan'] == 5
    assert merge['operationMetrics']['num_target_files_removed'] == 1
    assert merge['operationMetrics']['num_target_files_added'] == 2
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
