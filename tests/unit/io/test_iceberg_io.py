"""IcebergIO-specific tests, beyond test_io_contract.py and test_table_io.py."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pyiceberg.table import Table

import datetime
import os

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pfeed.io.base_io import DatasetKey, DatePartition
from pfeed.io.iceberg_io import IcebergIO
from pfeed.io.table_io import VacuumResult

KEY = DatasetKey(
    namespace={'env': 'BACKTEST', 'data_source': 'BYBIT'},
    name={'asset_type': 'PERPETUAL', 'resolution': '1t'},
    partition_by=('product', 'date'),
)
D1, D2, D3 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)


@pytest.fixture
def io(tmp_path: Path) -> IcebergIO:
    return IcebergIO(base_path=str(tmp_path))


@pytest.fixture
def data() -> pa.Table:
    """3 rows across 2 partitions of KEY: BTC/D1 (ts 1, 2) and BTC/D2 (ts 3)."""
    return pa.table({'ts': [1, 2, 3], 'product': ['BTC'] * 3, 'date': [D1, D1, D2]})


def _load_table(io: IcebergIO) -> Table:
    return io._load_catalog().load_table(('BACKTEST__BYBIT', 'PERPETUAL__1t'))


def _data_files_per_partition(table: Table) -> dict[tuple, int]:
    """Counts the data files (not marker files) of each partition."""
    counts: dict[tuple, int] = {}
    for partition in table.inspect.files().column('partition').to_pylist():
        if partition[IcebergIO.IS_METADATA_COLUMN]:
            continue
        values = (partition['product'], partition['date'])
        counts[values] = counts.get(values, 0) + 1
    return counts


def test_layout(io: IcebergIO, tmp_path: Path, data: pa.Table):
    """The catalog is <base_path>/pfeed.db, KEY's table is <schema>.<table>, partitioned by KEY.partition_by
    + IS_METADATA_COLUMN (so marker rows are in files of their own), with its files under
    <base_path>/warehouse/<schema>/<table>/.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})

    table = _load_table(io)
    assert (tmp_path / 'pfeed.db').is_file()
    assert table.location() == f'file://{tmp_path}/warehouse/BACKTEST__BYBIT/PERPETUAL__1t'
    assert [field.name for field in table.spec().fields] == ['product', 'date', io.IS_METADATA_COLUMN]


def test_optimize_compacts_partition_files(io: IcebergIO, data: pa.Table):
    """optimize() rewrites each partition with more than one data file into one; the others are left as they are.

    BTC/D1 is appended to twice, so it has a data file per write.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    for ts in (4, 5):
        io.write(KEY, pa.table({'ts': [ts], 'product': ['BTC'], 'date': [D1]}), partitions={('BTC', D1): {}}, mode='append')
    d2_files = {path for path in _load_table(io).inspect.files().column('file_path').to_pylist() if 'date=2025-01-02' in path}
    assert _data_files_per_partition(_load_table(io))[('BTC', D1)] > 1

    io.optimize()

    files = _load_table(io).inspect.files()
    assert _data_files_per_partition(_load_table(io)) == {('BTC', D1): 1, ('BTC', D2): 1}
    assert d2_files <= set(files.column('file_path').to_pylist())


def test_append_drops_marker_file(io: IcebergIO, data: pa.Table):
    """Append replaces a partition's marker row by dropping its marker file, so no data file is rewritten
    and appending doesn't get slower as a partition grows.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    files_before = set(_load_table(io).inspect.files().column('file_path').to_pylist())

    io.write(KEY, pa.table({'ts': [4], 'product': ['BTC'], 'date': [D1]}), partitions={('BTC', D1): {}}, mode='append')

    files_after = set(_load_table(io).inspect.files().column('file_path').to_pylist())
    (dropped,) = files_before - files_after
    assert f'{io.IS_METADATA_COLUMN}=true' in dropped
    assert len(files_after - files_before) == 2  # BTC/D1's new data file and marker file


def test_vacuum_keeps_recent_snapshots(io: IcebergIO, data: pa.Table):
    """vacuum() only expires snapshots replaced by a newer one more than `retention` ago,
    so time travel within `retention` keeps working; a dry run only reports what it would expire.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    io.write(KEY, data.filter(pc.field('date') == D1), partitions={('BTC', D1): {}})
    snapshot_ids = [snapshot.snapshot_id for snapshot in _load_table(io).snapshots()]

    assert io.vacuum(retention=datetime.timedelta(hours=1), dry_run=False) == VacuumResult()
    dry_run = io.vacuum(retention=datetime.timedelta(0))
    assert sorted(dry_run.expired_snapshots) == sorted(f'BACKTEST__BYBIT.PERPETUAL__1t@{snapshot_id}' for snapshot_id in snapshot_ids[:-1])
    assert dry_run.deleted_paths
    assert [snapshot.snapshot_id for snapshot in _load_table(io).snapshots()] == snapshot_ids

    assert io.vacuum(retention=datetime.timedelta(0), dry_run=False) == dry_run
    table = _load_table(io)
    current_snapshot = table.current_snapshot()
    assert current_snapshot is not None
    assert [snapshot.snapshot_id for snapshot in table.snapshots()] == [current_snapshot.snapshot_id]


def test_vacuum_deletes_orphans(io: IcebergIO, tmp_path: Path, data: pa.Table):
    """vacuum() deletes files under the table's location no snapshot uses (e.g. left by a crashed write),
    once older than retention; a recent one may belong to a write still committing, so it is kept.
    """
    io.write(KEY, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    orphan = tmp_path / 'warehouse' / 'BACKTEST__BYBIT' / 'PERPETUAL__1t' / 'data' / 'orphan.parquet'
    orphan.write_bytes(b'')

    assert io.vacuum(retention=datetime.timedelta(hours=1), dry_run=False) == VacuumResult()
    two_hours_ago = datetime.datetime.now().timestamp() - 2 * 3600
    os.utime(orphan, (two_hours_ago, two_hours_ago))
    assert io.vacuum(retention=datetime.timedelta(hours=1), dry_run=False) == VacuumResult(deleted_paths=[str(orphan)])
    assert not orphan.exists()


def test_date_partition_is_logical(io: IcebergIO, tmp_path: Path):
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

    assert [field.name for field in _load_table(io).spec().fields] == ['product', io.IS_METADATA_COLUMN]
    assert len([file for file in (tmp_path / 'warehouse').rglob('*.parquet') if f'{io.IS_METADATA_COLUMN}=false' in str(file)]) == 1

    io.write(key, data.slice(0, 1).set_column(2, 'price', pa.array([9.0])), partitions={('BTC', D1): {'version': 2}})

    lf, metadata = io.read(key)
    assert lf is not None
    assert lf.sort('date').collect()['price'].to_list() == [9.0, 3.0]
    assert metadata == {('BTC', D1): {'version': 2}, ('BTC', D3): {}}
