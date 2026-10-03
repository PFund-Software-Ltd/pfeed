from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

import datetime
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

type PartitionValue = str | int | datetime.date
# partition values, positionally aligned with DatasetKey.partition_by, e.g. ('BTC_USDT_PERP', date(2025, 1, 1))
type Partition = tuple[PartitionValue, ...]
# must be JSON-safe (str keys; str/int/float/bool/None/list/dict values), convert e.g. dates to str before writing
type Metadata = dict[str, Any]


@dataclass(frozen=True)
class DatePartition:
    """A partition level whose value is the calendar date (UTC) of a timestamp/date column.

    The value is computed from `column` by the IO, not stored as a column of its own,
    e.g. DatePartition('date') puts a row with date=2025-01-01 01:00:00 in partition date(2025, 1, 1).

    Attributes:
        column: the timestamp/date column the date is computed from.
    """

    column: str


# a column whose values are the partition values, or a DatePartition computing them from a column
type PartitionLevel = str | DatePartition


@dataclass(frozen=True)
class DatasetKey:
    """Logical identity of a dataset, independent of where/how it is stored.

    Each IO maps it to its own physical layout, e.g.
    - ParquetIO: hive dirs `k=v/.../k=v/` for namespace + name, then one dir per partition
    - TableIOs (DuckLakeIO, DeltaLakeIO, IcebergIO): schema = namespace values joined by '__',
        table = name values joined by '__'

    Attributes:
        namespace: ordered (key -> value), e.g. {'env': 'BACKTEST', 'data_layer': 'CLEANED', ...}
        name: ordered (key -> value), e.g. {'asset_type': 'PERPETUAL', 'resolution': '1t'}
        partition_by: partition levels of the dataset, e.g. ('product', DatePartition('date')).
            Empty for unpartitioned datasets.
            A partition is a logical unit of write (replace), metadata and existence, not a physical layout;
            each IO decides how (and whether) it is reflected in storage.
    """

    namespace: dict[str, str]
    name: dict[str, str]
    partition_by: tuple[PartitionLevel, ...] = field(default=())

    @property
    def partition_columns(self) -> list[str]:
        """The data columns the partition values are read or computed from."""
        return [level.column if isinstance(level, DatePartition) else level for level in self.partition_by]


@dataclass(frozen=True)
class IOCapabilities:
    """What an IO supports, so callers can check before writing.

    Attributes:
        append: supports write(mode='append').
        concurrent_partition_writes: multiple processes (e.g. Ray workers) may call write() on the
            same DatasetKey at the same time, as long as they cover disjoint partitions.
            Concurrent writes to the same partition are not covered by any IO; avoid them.
    """

    append: bool = False
    concurrent_partition_writes: bool = False


class BaseIO(ABC):
    """Reads/writes dataframes by DatasetKey.

    IO knows nothing about the data's domain (no product/date/resolution);
    partitioning is whatever DatasetKey.partition_by declares.

    Metadata is the commit marker: a partition exists if and only if it has metadata.
    IOs must make metadata visible no earlier than its data (same transaction if supported,
    otherwise data first, metadata last), so a crash in between leaves the partition "missing"
    and the next 'replace' overwrites the leftover data.
    Metadata is opaque to IO; the data handler owns its schema.
    """

    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities()

    @staticmethod
    def _dump_metadata(metadata: Metadata) -> str:
        """Serializes metadata to JSON, raising TypeError if it wouldn't come back exactly as given.

        json.dumps alone raises on e.g. dates, but silently changes tuples to lists,
        int keys to str keys and writes NaN, so the round-trip is checked too.
        """
        dumped = json.dumps(metadata)
        if json.loads(dumped) != metadata:
            raise TypeError(f'metadata is not JSON-safe, it would not round-trip exactly: {metadata!r}')
        return dumped

    @staticmethod
    def _partition_value_arrays(key: DatasetKey, data: pa.Table) -> list[pa.Array]:
        """Returns each row's partition values, one array per level of key.partition_by.

        Raises ValueError if data is missing a partition column,
        TypeError if a DatePartition column isn't a timestamp/date.
        """
        import pyarrow as pa

        if missing_cols := set(key.partition_columns) - set(data.column_names):
            raise ValueError(f'data is missing partition columns {missing_cols}')
        arrays = []
        for level in key.partition_by:
            if isinstance(level, DatePartition):
                column = data[level.column].combine_chunks()
                if pa.types.is_timestamp(column.type):
                    # dropping the time zone keeps the UTC instant, so the date is the UTC date
                    column = column.cast(pa.timestamp(column.type.unit))
                elif not pa.types.is_date(column.type):
                    raise TypeError(f'{level} needs a timestamp/date column, got {column.type}')
                arrays.append(column.cast(pa.date32()))
            else:
                arrays.append(data[level].combine_chunks())
        return arrays

    @classmethod
    def _data_partitions(cls, key: DatasetKey, data: pa.Table, partitions: dict[Partition, Metadata]) -> set[Partition]:
        """Returns the partitions that have rows in data, checking write()'s `data` and `partitions` requirements.

        Raises ValueError if data is missing a partition column or has a partition not in `partitions`.
        """
        import pyarrow as pa

        arrays = cls._partition_value_arrays(key, data)
        if key.partition_by:
            levels = [str(i) for i in range(len(arrays))]
            unique_rows = pa.table(dict(zip(levels, arrays, strict=True))).group_by(levels).aggregate([]).to_pylist()
            data_partitions = {tuple(row[level] for level in levels) for row in unique_rows}
        else:
            data_partitions = {()} if data.num_rows else set()
        if not_in_partitions := data_partitions - partitions.keys():
            raise ValueError(f'partitions {not_in_partitions} in data are not in `partitions`')
        return data_partitions

    @abstractmethod
    def write(
        self,
        key: DatasetKey,
        data: pa.Table,
        *,
        partitions: dict[Partition, Metadata],
        mode: Literal['replace', 'append'] = 'replace',
    ) -> None:
        """Write data and its per-partition metadata to the dataset.

        Args:
            key: dataset to write to.
            data: must contain all `key.partition_columns`.
                Its columns may differ from the dataset's existing columns (schema drift):
                - a new column is added to the dataset; existing rows read it as null
                - a missing column is null in this write's rows; the dataset never loses a column
                - a column whose type differs from the dataset's raises TypeError and writes nothing
            partitions: the partitions this write covers, each with its metadata.
                Every partition in `data` must be in it. A partition in it but with no rows
                in `data` is a valid empty partition (e.g. a date that was fetched but had no trades).
                Existing metadata of these partitions is replaced, not merged.
            mode:
                - 'replace': overwrite `partitions` (dynamic partition overwrite);
                    an empty partition's existing rows are deleted. Other partitions are untouched.
                - 'append': add rows to `partitions`.
                    Only if CAPABILITIES.append, otherwise raises NotImplementedError.
        """

    @abstractmethod
    def read(
        self,
        key: DatasetKey,
        *,
        partitions: list[Partition] | None = None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        """Read the dataset and its per-partition metadata.

        Metadata has to be read anyway to find the existing partitions (see commit marker),
        and the LazyFrame loads nothing until collected, so metadata-only callers can just
        ignore the frame, e.g. `_, metadata = io.read(key, partitions=partitions)`.

        Args:
            key: dataset to read.
            partitions: partitions to read; None reads the whole dataset.

        Returns:
            (data, metadata):
            - data: LazyFrame including the `key.partition_columns`, only from partitions that have metadata;
                None if the dataset does not exist or none of the partitions exist or all of them are empty.
                Has every column ever written to the dataset, null where a partition doesn't have it.
            - metadata: metadata of the existing partitions; {} if none.
                Requested partitions not in it are missing.
        """
