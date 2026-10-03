"""Contract tests every BaseIO implementation must pass."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.io.base_io import BaseIO, Metadata, Partition

import datetime
from functools import partial

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from polars.testing import assert_frame_equal

from pfeed.io.base_io import DatasetKey
from pfeed.io.ducklake_io import DuckLakeIO
from pfeed.io.parquet_io import ParquetIO


@pytest.fixture(params=[
    partial(ParquetIO),
    partial(DuckLakeIO),
    # the test data is small enough to be inlined into the catalog, so also test with every write as parquet files
    partial(DuckLakeIO, data_inlining_row_limit=0),
], ids=['ParquetIO', 'DuckLakeIO', 'DuckLakeIO-no_inlining'])
def io(request, tmp_path) -> BaseIO:
    return request.param(base_path=str(tmp_path))


KEY = DatasetKey(
    namespace={'env': 'BACKTEST', 'data_source': 'BYBIT'},
    name={'asset_type': 'PERPETUAL', 'resolution': '1t'},
    partition_by=('product', 'date'),
)
D1, D2, D3 = datetime.date(2025, 1, 1), datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)


@pytest.fixture
def data() -> pa.Table:
    """3 rows across 2 partitions of KEY: BTC/D1 (ts 1, 2) and BTC/D2 (ts 3)."""
    return pa.table({
        'ts': [1, 2, 3],
        'price': [100.0, 101.0, 102.0],
        'product': ['BTC', 'BTC', 'BTC'],
        'date': [D1, D1, D2],
    })


def test_roundtrip(io: BaseIO, data: pa.Table):
    """What is written comes back unchanged: same rows, dtypes and per-partition metadata
    (any JSON types, see test_non_json_metadata_raises for the rest).

    Writes `data` (BTC/D1 with 2 rows, BTC/D2 with 1 row), then reads the whole dataset back.
    Rows are sorted by ts since read order isn't guaranteed.
    """
    # every JSON type, nested too
    metadata: Metadata = {
        'str': 'a',
        'int': 1,
        'float': 1.5,
        'bool': True,
        'none': None,
        'list': [1, 'a'],
        'dict': {'nested': {'deep': [1, 2]}},
    }
    partitions: dict[Partition, Metadata] = {('BTC', D1): metadata, ('BTC', D2): metadata | {'int': 2}}

    io.write(KEY, data, partitions=partitions)
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == partitions
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_read_missing_dataset(io: BaseIO):
    """Reading a dataset that was never written returns (None, {}) instead of raising.

    Covers both reading the whole dataset and reading specific partitions.
    """
    assert io.read(KEY) == (None, {})
    assert io.read(KEY, partitions=[('BTC', D1)]) == (None, {})


def test_replace_only_touches_given_partitions(io: BaseIO, data: pa.Table):
    """Replace overwrites only the given partitions (dynamic partition overwrite).

    Writes `data` (BTC/D1 and BTC/D2), then replaces BTC/D1 with new rows and metadata.
    BTC/D1 must be fully replaced (old rows gone, metadata not merged); BTC/D2 must be untouched.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    new_data = pa.table({
        'ts': [10],
        'price': [200.0],
        'product': ['BTC'],
        'date': [D1],
    })
    new_partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 2}}
    io.write(KEY, new_data, partitions=new_partitions, mode='replace')
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 2}, ('BTC', D2): {'version': 1}}
    d2_untouched = data.filter(pc.field('date') == D2)
    expected = pa.concat_tables([d2_untouched, new_data])
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(expected))


def test_empty_partition(io: BaseIO, data: pa.Table):
    """A partition in `partitions` with no rows in data is an empty partition: it exists, but has no rows.

    Writes `data` (BTC/D1 and BTC/D2), then replaces BTC/D1 with no rows,
    e.g. a re-download found no trades for that date.
    - whole dataset: BTC/D1's old rows are gone, but its metadata is still there
    - BTC/D1 only: no rows to read, so data is None, but the partition exists (has metadata)
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    empty_partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 2}}
    io.write(KEY, data.schema.empty_table(), partitions=empty_partitions)

    lf, read_metadata = io.read(KEY)
    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 2}, ('BTC', D2): {'version': 1}}
    d2_untouched = data.filter(pc.field('date') == D2)
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(d2_untouched))

    lf, read_metadata = io.read(KEY, partitions=[('BTC', D1)])
    assert lf is None
    assert read_metadata == {('BTC', D1): {'version': 2}}


def test_empty_metadata(io: BaseIO, data: pa.Table):
    """Empty metadata {} still commits the partition: it is metadata with no fields, not "no metadata".

    Writes `data` (BTC/D1 and BTC/D2) with {} for both, then reads the whole dataset back.
    Both partitions must exist with {} as their metadata, and all rows must be there.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {}, ('BTC', D2): {}}

    io.write(KEY, data, partitions=partitions)
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == partitions
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_read_subset_partitions(io: BaseIO, data: pa.Table):
    """Reading specific partitions returns only those; requested partitions that don't exist are just left out.

    Writes `data` (BTC/D1 and BTC/D2), then reads BTC/D1 and BTC/D3 (never written).
    - data: only BTC/D1's rows, nothing from BTC/D2
    - metadata: only BTC/D1; BTC/D3 is missing, so the caller knows it still needs downloading
    Reading only missing partitions, or no partitions at all, returns (None, {}).
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    lf, read_metadata = io.read(KEY, partitions=[('BTC', D1), ('BTC', D3)])

    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 1}}
    d1_only = data.filter(pc.field('date') == D1)
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(d1_only))
    assert io.read(KEY, partitions=[('BTC', D3)]) == (None, {})
    assert io.read(KEY, partitions=[]) == (None, {})


def test_unpartitioned(io: BaseIO, data: pa.Table):
    """A dataset with no partition_by is one single partition, keyed by ().

    Writes `data` (3 rows), then replaces it with its last row.
    Since the whole dataset is one partition, replace swaps out the whole dataset.
    """
    unpartitioned_key = DatasetKey(namespace=KEY.namespace, name=KEY.name)
    io.write(unpartitioned_key, data, partitions={(): {'version': 1}})
    lf, read_metadata = io.read(unpartitioned_key)

    assert lf is not None
    assert read_metadata == {(): {'version': 1}}
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))

    new_data = data.slice(2)
    io.write(unpartitioned_key, new_data, partitions={(): {'version': 2}})
    lf, read_metadata = io.read(unpartitioned_key)

    assert lf is not None
    assert read_metadata == {(): {'version': 2}}
    assert_frame_equal(lf.collect(), pl.DataFrame(new_data))


@pytest.mark.parametrize('partition_by', [KEY.partition_by, ()], ids=['partitioned', 'unpartitioned'])
def test_write_no_partitions_is_noop(io: BaseIO, data: pa.Table, partition_by: tuple[str, ...]):
    """Writing with empty `partitions` (and so no rows) writes nothing and leaves the dataset as it was."""
    key = DatasetKey(namespace=KEY.namespace, name=KEY.name, partition_by=partition_by)
    partitions: dict[Partition, Metadata] = {('BTC', D1): {}, ('BTC', D2): {}} if partition_by else {(): {}}
    io.write(key, data, partitions=partitions)

    io.write(key, data.schema.empty_table(), partitions={})
    lf, read_metadata = io.read(key)

    assert lf is not None
    assert read_metadata == partitions
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_data_partition_not_in_partitions_raises(io: BaseIO, data: pa.Table):
    """Every partition that has rows in data must be in `partitions`, otherwise write raises and writes nothing.

    Writes `data` (BTC/D1 and BTC/D2), then writes BTC/D1 rows but only lists BTC/D2 in `partitions`.
    The write is rejected as a whole: BTC/D1 isn't written without metadata, and BTC/D2 isn't
    wiped as an empty partition. The dataset stays as it was.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    d1_only = data.filter(pc.field('date') == D1)
    with pytest.raises(ValueError):
        io.write(KEY, d1_only, partitions={('BTC', D2): {'version': 2}})
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == partitions
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_append_unsupported_raises(io: BaseIO, data: pa.Table):
    """IOs without CAPABILITIES.append must raise NotImplementedError on mode='append' and write nothing."""
    if io.CAPABILITIES.append:
        pytest.skip(f'{type(io).__name__} supports append')
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}

    with pytest.raises(NotImplementedError):
        io.write(KEY, data, partitions=partitions, mode='append')

    assert io.read(KEY) == (None, {})


@pytest.mark.parametrize('other_key', [
    # sibling: differs in one name value
    DatasetKey(
        namespace=KEY.namespace,
        name=KEY.name | {'resolution': '1m'},
        partition_by=KEY.partition_by,
    ),
    # parent: KEY's name with 'resolution' dropped, so in a path-based IO
    # KEY's dataset lives *inside* this one's dataset dir
    DatasetKey(
        namespace=KEY.namespace,
        name={'asset_type': KEY.name['asset_type']},
        partition_by=KEY.partition_by,
    ),
], ids=['sibling', 'parent'])
def test_datasets_are_isolated(io: BaseIO, data: pa.Table, other_key: DatasetKey):
    """Writing one dataset is invisible to another, even with the same partitions.

    Writes `data` to KEY only, then reads other_key: it must be (None, {}).
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    assert io.read(other_key) == (None, {})
    assert io.read(other_key, partitions=[('BTC', D1)]) == (None, {})


@pytest.mark.parametrize('bad_metadata', [
    {'date': D1},
    {'tuple': (1, 2)},
    {'nan': float('nan')},
    {'nested': {1: 'int key'}},
], ids=['date', 'tuple', 'nan', 'int_key'])
def test_non_json_metadata_raises(io: BaseIO, data: pa.Table, bad_metadata: Metadata):
    """Metadata that wouldn't come back exactly as written raises TypeError and writes nothing.

    Dates must be converted to str by the caller; tuples, NaN and int keys would be silently
    changed by JSON, so they are rejected too. BTC/D1's metadata is valid and BTC/D2's isn't,
    so nothing must be written, not even BTC/D1.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): bad_metadata}

    with pytest.raises(TypeError):
        io.write(KEY, data, partitions=partitions)

    assert io.read(KEY) == (None, {})


def test_schema_drift(io: BaseIO, data: pa.Table):
    """Columns may come and go across writes; the dataset keeps every column ever written.

    Writes BTC/D1 with an extra 'RPI' column, then BTC/D2 without it, then BTC/D3 with a new 'tick' column,
    e.g. a vendor dropping and adding columns over time.
    Every read has all columns, with nulls where a partition doesn't have them,
    including a read of a single partition.
    """
    d1 = data.filter(pc.field('date') == D1).append_column('RPI', pa.array([True, False]))
    d2 = data.filter(pc.field('date') == D2)
    d3 = pa.table({'ts': [4], 'price': [103.0], 'product': ['BTC'], 'date': [D3], 'tick': ['PlusTick']})
    io.write(KEY, d1, partitions={('BTC', D1): {'version': 1}})
    io.write(KEY, d2, partitions={('BTC', D2): {'version': 1}})
    io.write(KEY, d3, partitions={('BTC', D3): {'version': 1}})

    lf, _ = io.read(KEY)
    assert lf is not None
    expected = pl.DataFrame({
        'ts': [1, 2, 3, 4],
        'price': [100.0, 101.0, 102.0, 103.0],
        'product': ['BTC'] * 4,
        'date': [D1, D1, D2, D3],
        'RPI': [True, False, None, None],
        'tick': [None, None, None, 'PlusTick'],
    })
    assert_frame_equal(lf.collect().sort('ts'), expected, check_column_order=False)

    lf, _ = io.read(KEY, partitions=[('BTC', D2)])
    assert lf is not None
    assert_frame_equal(lf.collect(), expected.filter(pl.col('date') == D2), check_column_order=False)


def test_column_type_change_raises(io: BaseIO, data: pa.Table):
    """A column whose type differs from the dataset's raises TypeError and writes nothing.

    Writes `data` (price is float), then writes BTC/D3 with price as str.
    The dataset stays as it was, BTC/D3 doesn't exist.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    d3 = pa.table({'ts': [4], 'price': ['103.0'], 'product': ['BTC'], 'date': [D3]})
    with pytest.raises(TypeError):
        io.write(KEY, d3, partitions={('BTC', D3): {'version': 1}})
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == partitions
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(data))


def test_missing_partition_column_raises(io: BaseIO, data: pa.Table):
    """Data must contain every partition_by column, otherwise write raises ValueError and writes nothing.

    Drops 'date' from `data`, so the IO can't tell which partition each row belongs to.
    """
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}

    with pytest.raises(ValueError):
        io.write(KEY, data.drop_columns(['date']), partitions=partitions)

    assert io.read(KEY) == (None, {})
