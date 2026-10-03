"""DuckLakeIO-specific tests, beyond test_io_contract.py and test_table_io.py."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import datetime

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
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


def test_reserved_metadata_suffix_raises(io: DuckLakeIO, data: pa.Table):
    """A dataset name ending with the metadata table suffix would clash with another dataset's metadata table."""
    key = DatasetKey(namespace=KEY.namespace, name=KEY.name | {'kind': 'metadata'}, partition_by=KEY.partition_by)

    with pytest.raises(ValueError):
        io.write(key, data, partitions={('BTC', D1): {}, ('BTC', D2): {}})
    with pytest.raises(ValueError):
        io.read(key)


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


def test_vacuum_is_two_step(tmp_path: Path, data: pa.Table):
    """vacuum() deletes files that old snapshots used, but only a retention period after expiring them,
    so with retention=0 the first vacuum expires the snapshots and the second deletes their files.
    A dry run reports the snapshots the first vacuum would expire, though it deletes no snapshot files yet.

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

    dry_run = io.vacuum(retention=datetime.timedelta(0))
    assert dry_run.expired_snapshots
    assert dry_run.deleted_paths == [str(orphan)]
    assert {str(path) for path in tmp_path.rglob('*.parquet')} == files_before

    first = io.vacuum(retention=datetime.timedelta(0), dry_run=False)
    second = io.vacuum(retention=datetime.timedelta(0), dry_run=False)
    lf, _ = io.read(KEY)

    assert first.expired_snapshots == dry_run.expired_snapshots
    assert first.deleted_paths == [str(orphan)]
    assert second.expired_snapshots == []
    first_paths, second_paths = first.deleted_paths, second.deleted_paths
    assert second_paths  # the replaced BTC/D1 file
    assert {str(path) for path in tmp_path.rglob('*.parquet')} == files_before - {*first_paths, *second_paths}
    assert lf is not None
    expected = pa.concat_tables([new_data, data.filter(pc.field('date') == D2)])
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(expected).sort('ts'))
