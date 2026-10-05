"""Contract tests every BaseIO implementation must pass."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.io.base_io import BaseIO, Metadata, Partition

import datetime
import multiprocessing
from functools import partial

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from polars.testing import assert_frame_equal

from pfeed.io.base_io import DatasetKey, DatePartition
from pfeed.io.deltalake_io import DeltaLakeIO
from pfeed.io.ducklake_io import DuckLakeIO
from pfeed.io.iceberg_io import IcebergIO
from pfeed.io.parquet_io import ParquetIO


@pytest.fixture(params=[
    partial(ParquetIO),
    partial(DuckLakeIO),
    # the test data is small enough to be inlined into the catalog, so also test with every write as parquet files
    partial(DuckLakeIO, data_inlining_row_limit=0),
    partial(DeltaLakeIO),
    partial(IcebergIO),
], ids=['ParquetIO', 'DuckLakeIO', 'DuckLakeIO-no_inlining', 'DeltaLakeIO', 'IcebergIO'])
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


def test_append(io: BaseIO, data: pa.Table):
    """Append adds rows to the given partitions; an existing partition keeps its metadata,
    a new one is created with its metadata; other partitions are untouched.

    Writes `data` (BTC/D1 and BTC/D2), then appends a row to BTC/D1 (existing) and one to BTC/D3 (new), with new metadata.
    """
    if not io.CAPABILITIES.append:
        pytest.skip(f'{type(io).__name__} does not support append')
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions)

    new_data = pa.table({'ts': [4, 5], 'price': [103.0, 104.0], 'product': ['BTC', 'BTC'], 'date': [D1, D3]})
    io.write(KEY, new_data, partitions={('BTC', D1): {'version': 2}, ('BTC', D3): {'version': 2}}, mode='append')
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}, ('BTC', D3): {'version': 2}}
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(pa.concat_tables([data, new_data])))


def test_append_to_new_partition(io: BaseIO, data: pa.Table):
    """Appending to a partition that doesn't exist yet creates it, also when the dataset doesn't exist yet.

    Appends `data` (BTC/D1 and BTC/D2) to a new dataset, then a row to BTC/D3.
    """
    if not io.CAPABILITIES.append:
        pytest.skip(f'{type(io).__name__} does not support append')
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    io.write(KEY, data, partitions=partitions, mode='append')

    new_data = pa.table({'ts': [4], 'price': [103.0], 'product': ['BTC'], 'date': [D3]})
    io.write(KEY, new_data, partitions={('BTC', D3): {'version': 1}}, mode='append')
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == partitions | {('BTC', D3): {'version': 1}}
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(pa.concat_tables([data, new_data])))


def test_concurrent_partition_writes(io: BaseIO):
    """Processes writing disjoint partitions of the same dataset at the same time all land.

    A pool of 4 processes writes 20 partitions, one write each, with partition i having i + 1 rows.
    Some IOs detect conflicts per table, so these writes may conflict and must be retried.
    The io is pickled into each process (spawn), like a Ray worker.
    """
    if not io.CAPABILITIES.concurrent_partition_writes:
        pytest.skip(f'{type(io).__name__} does not support concurrent partition writes')
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


def test_schema_drift_append(io: BaseIO, data: pa.Table):
    """Schema drift works the same when appending: a new column is added, a missing one is null.

    Writes BTC/D1 with an extra 'RPI' column, then appends a row to BTC/D1 without 'RPI' but with a new 'tick' column.
    A column whose type differs from the dataset's still raises TypeError and appends nothing.
    """
    if not io.CAPABILITIES.append:
        pytest.skip(f'{type(io).__name__} does not support append')
    d1 = data.filter(pc.field('date') == D1).append_column('RPI', pa.array([True, False]))
    io.write(KEY, d1, partitions={('BTC', D1): {'version': 1}})
    new_row = pa.table({'ts': [4], 'price': [103.0], 'product': ['BTC'], 'date': [D1], 'tick': ['PlusTick']})
    io.write(KEY, new_row, partitions={('BTC', D1): {'version': 2}}, mode='append')

    with pytest.raises(TypeError):
        io.write(KEY, new_row.set_column(1, 'price', pa.array(['103.0'])), partitions={('BTC', D1): {'version': 3}}, mode='append')
    lf, read_metadata = io.read(KEY)

    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 1}}  # append keeps an existing partition's metadata
    expected = pl.DataFrame({
        'ts': [1, 2, 4],
        'price': [100.0, 101.0, 103.0],
        'product': ['BTC'] * 3,
        'date': [D1] * 3,
        'RPI': [True, False, None],
        'tick': [None, None, 'PlusTick'],
    })
    assert_frame_equal(lf.collect().sort('ts'), expected, check_column_order=False)


def test_timestamps(io: BaseIO, data: pa.Table):
    """Timestamps come back unchanged, with or without a time zone (UTC; IOs may store others in UTC).

    Nanosecond timestamps either come back unchanged too, or, where the format only supports
    microseconds (Delta Lake, Iceberg), come back as microseconds if that loses nothing,
    else raise TypeError and write nothing, instead of being truncated.
    """
    d1 = data.filter(pc.field('date') == D1)
    us = datetime.datetime(2025, 1, 1, 1, 2, 3, 456789)
    d1 = d1.append_column('us', pa.array([us, us], pa.timestamp('us')))
    d1 = d1.append_column('utc', pa.array([us, us], pa.timestamp('us', tz='UTC')))
    io.write(KEY, d1, partitions={('BTC', D1): {}})
    lf, _ = io.read(KEY)

    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), pl.DataFrame(d1))

    # ns of whole microseconds, e.g. upcast from us: every IO stores it without loss
    d2 = data.filter(pc.field('date') == D2)
    padded_ns = pa.array([1_735_693_323_456_789_000], pa.timestamp('ns'))
    io.write(KEY, d2.append_column('ns', padded_ns), partitions={('BTC', D2): {}})
    lf, _ = io.read(KEY, partitions=[('BTC', D2)])
    assert lf is not None
    assert lf.collect()['ns'].to_list() == pl.Series(padded_ns).to_list()

    d3 = d2.set_column(d2.schema.get_field_index('date'), 'date', pa.array([D3]))
    ns = pa.array([1_735_693_323_456_789_123], pa.timestamp('ns'))
    d3 = d3.append_column('ns', ns)
    try:
        io.write(KEY, d3, partitions={('BTC', D3): {}})
    except TypeError:
        assert io.read(KEY, partitions=[('BTC', D3)]) == (None, {})
    else:
        lf, _ = io.read(KEY, partitions=[('BTC', D3)])
        assert lf is not None
        assert lf.collect()['ns'].to_list() == pl.Series(ns).to_list()


def test_timestamp_precision_loss_allowed(io: BaseIO, data: pa.Table, monkeypatch: pytest.MonkeyPatch):
    """With the config's allow_timestamp_precision_loss, IOs that store microseconds truncate ns with a warning."""
    from pfeed.config import get_config

    monkeypatch.setattr(get_config(), 'allow_timestamp_precision_loss', True)
    ns = pa.array([1_735_693_323_456_789_123], pa.timestamp('ns'))
    d2 = data.filter(pc.field('date') == D2).append_column('ns', ns)
    stores_us = isinstance(io, (DeltaLakeIO, IcebergIO))
    if stores_us:
        with pytest.warns(RuntimeWarning, match="truncated 1 timestamps of column 'ns'"):
            io.write(KEY, d2, partitions={('BTC', D2): {}})
    else:
        io.write(KEY, d2, partitions={('BTC', D2): {}})
    lf, _ = io.read(KEY, partitions=[('BTC', D2)])
    assert lf is not None
    expected = pl.Series(ns).dt.truncate('1us') if stores_us else pl.Series(ns)
    assert lf.collect()['ns'].dt.cast_time_unit('ns').to_list() == expected.to_list()


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


DATE_KEY = DatasetKey(namespace=KEY.namespace, name=KEY.name, partition_by=('product', DatePartition('date')))


@pytest.mark.parametrize('tz', [None, 'UTC', 'America/New_York'], ids=['naive', 'utc', 'new_york'])
def test_date_partition(io: BaseIO, tz: str | None):
    """A DatePartition level partitions rows by the UTC date of a timestamp column, whatever its time zone,
    and the data comes back with only its own columns (whatever an IO stores to partition it is dropped).

    Writes BTC rows on D1 (2 rows, the 2nd one at 23:30 UTC, still D1 in UTC but not in New York's local date
    for a 01:00 UTC row) and D3 (1 row), plus D2 as an empty partition, then checks:
    - read: the data comes back unchanged and partitions are keyed by date, D2 included
    - read of a subset: only that date's rows and metadata
    - replace: rewriting D1 leaves D3 untouched
    """
    if tz not in (None, 'UTC') and isinstance(io, (DeltaLakeIO, IcebergIO)):
        pytest.skip(f'{type(io).__name__} only stores timestamps without a time zone or in UTC')
    # us, so the table formats storing microseconds keep them as they are
    dtype = pa.timestamp('us', tz=tz)
    data = pa.table({
        'date': pa.array([
            datetime.datetime(2025, 1, 1, 1), datetime.datetime(2025, 1, 1, 23, 30), datetime.datetime(2025, 1, 3, 5),
        ], pa.timestamp('us')).cast(dtype),
        'product': ['BTC', 'BTC', 'BTC'],
        'price': [1.0, 2.0, 3.0],
    })
    partitions: dict[Partition, Metadata] = {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}, ('BTC', D3): {'version': 1}}

    io.write(DATE_KEY, data, partitions=partitions)

    lf, read_metadata = io.read(DATE_KEY)
    assert lf is not None
    assert read_metadata == partitions
    df, expected = lf.collect().sort('price'), pl.DataFrame(data)
    if tz is not None:
        # IOs may store other time zones in UTC (see test_timestamps), so compare the instants
        df, expected = (frame.with_columns(pl.col('date').dt.convert_time_zone('UTC')) for frame in (df, expected))
    assert_frame_equal(df, expected)

    lf, read_metadata = io.read(DATE_KEY, partitions=[('BTC', D1), ('BTC', D2)])
    assert lf is not None
    assert read_metadata == {('BTC', D1): {'version': 1}, ('BTC', D2): {'version': 1}}
    assert lf.collect()['price'].sort().to_list() == [1.0, 2.0]

    new_d1 = data.slice(0, 1).set_column(2, 'price', pa.array([10.0]))
    io.write(DATE_KEY, new_d1, partitions={('BTC', D1): {'version': 2}})
    lf, read_metadata = io.read(DATE_KEY)
    assert lf is not None
    assert read_metadata == partitions | {('BTC', D1): {'version': 2}}
    assert lf.collect()['price'].sort().to_list() == [3.0, 10.0]
