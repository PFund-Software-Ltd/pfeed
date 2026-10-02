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
class DatasetKey:
    """Logical identity of a dataset, independent of where/how it is stored.

    Each IO maps it to its own physical layout, e.g.
    - ParquetIO: hive dirs `k=v/.../k=v/` for namespace + name, then one dir per partition
    - DuckLakeIO: schema = joined namespace values, table = joined name values

    Attributes:
        namespace: ordered (key -> value), e.g. {'env': 'BACKTEST', 'data_layer': 'CLEANED', ...}
        name: ordered (key -> value), e.g. {'asset_type': 'PERPETUAL', 'resolution': '1t'}
        partition_by: column names the dataset is partitioned by, e.g. ('product', 'date').
            Empty for unpartitioned datasets.
    """

    namespace: dict[str, str]
    name: dict[str, str]
    partition_by: tuple[str, ...] = field(default=())


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
            data: must contain all `key.partition_by` columns.
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
            - data: LazyFrame including the `key.partition_by` columns, only from partitions that have metadata;
                None if the dataset does not exist or none of the partitions exist or all of them are empty.
                Has every column ever written to the dataset, null where a partition doesn't have it.
            - metadata: metadata of the existing partitions; {} if none.
                Requested partitions not in it are missing.
        """
