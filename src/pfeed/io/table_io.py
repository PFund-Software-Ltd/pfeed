from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

    from pfeed.io.base_io import DatasetKey, Metadata, Partition

import datetime
import json
import operator
import os
import random
import time
import warnings
from abc import abstractmethod
from dataclasses import dataclass, field
from functools import reduce
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa

from pfeed.io.base_io import BaseIO, DatePartition, IOCapabilities


@dataclass(frozen=True)
class VacuumResult:
    """What TableIO.vacuum() expired and deleted, or would if dry_run.

    Attributes:
        expired_snapshots: the expired snapshots, time travel to them is no longer possible;
            DuckLake: the snapshot id, Iceberg: '<schema>.<table>@<snapshot id>',
            Delta Lake: always empty, it expires old versions itself (see delta.logRetentionDuration).
        deleted_paths: the deleted files.
    """

    expired_snapshots: list[str] = field(default_factory=list)
    deleted_paths: list[str] = field(default_factory=list)


class TableIO(BaseIO):
    """Base of the IOs storing each dataset as one table of a table format (DuckLake, Delta Lake, Iceberg).

    What they all share:
    - DatasetKey -> (schema, table): namespace values joined by NAME_SEPARATOR, name values joined by it,
        e.g. ("BACKTEST__BYBIT", "PERPETUAL__1t"). A key differing from an existing dataset only by case raises,
        since names are case-insensitive somewhere along the way (DuckDB names, macOS paths).
    - writes are one transaction each, metadata included, so a partition's data and metadata change together.
        Concurrent writes to disjoint partitions may conflict, a conflicting transaction is retried as a whole.
    - read() pins the data to the table version its metadata was read at, so they always match.
    - nothing is maintained automatically: run optimize() to compact (e.g. after many small writes),
        then vacuum() to delete what old versions no longer need.

    Formats without multi-table transactions (Delta Lake, Iceberg) keep the metadata in the data table itself,
    as one marker row per partition, see _add_marker_rows().
    Their columns METADATA_COLUMN and IS_METADATA_COLUMN are reserved, so data can't have them (in any TableIO).

    A DatePartition level is logical only, the tables are physically partitioned by the column levels:
    a day is matched by the range [day, day + 1) on its column, and reads skip other days' files by their
    min/max stats, since data is written in date order. Partitioning by day would leave tiny files
    (e.g. one daily bar per file), which compaction can't merge across partitions.
    """

    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities(append=True, concurrent_partition_writes=True)
    DEFAULT_DIR_NAME: ClassVar[str]
    # '__' instead of '_', since key values like 'market_data' contain '_'
    NAME_SEPARATOR: ClassVar[str] = '__'
    METADATA_COLUMN: ClassVar[str] = 'pfeed_metadata'
    IS_METADATA_COLUMN: ClassVar[str] = 'pfeed_is_metadata'
    # same as Delta Lake's default
    DEFAULT_VACUUM_RETENTION: ClassVar[datetime.timedelta] = datetime.timedelta(days=7)
    # retries of a write/maintenance transaction that conflicted with a concurrent one,
    # waiting RETRY_WAIT * RETRY_BACKOFF**attempt (with jitter) in between
    MAX_RETRIES: ClassVar[int] = 10
    RETRY_WAIT: ClassVar[float] = 0.1
    RETRY_BACKOFF: ClassVar[float] = 1.5
    # exceptions meaning the transaction conflicted with a concurrent one and can be retried
    _RETRY_ON: ClassVar[tuple[type[Exception], ...]]

    def __init__(self, base_path: str | None = None):
        """
        Args:
            base_path: root directory of the tables. Local paths only for now.
                Defaults to <config.data_path>/<DEFAULT_DIR_NAME>.
        """
        if base_path is None:
            from pfeed.config import get_config

            base_path = str(get_config().data_path / self.DEFAULT_DIR_NAME)
        if urlparse(base_path).scheme not in ('', 'file'):
            raise NotImplementedError(f'only local paths are supported for now, got {base_path!r}')
        self._base_path = os.path.abspath(base_path.removeprefix('file://'))

    def _table_names(self, key: DatasetKey) -> tuple[str, str]:
        """Returns (schema, table) of the dataset.

        Raises ValueError if a key value could make two different keys join to the same name,
        e.g. ('A_', 'B') and ('A', '_B') would both join to 'A___B',
        or can't be a directory name, since some formats store each table in <schema>/<table>/.
        """
        sep = self.NAME_SEPARATOR
        for value in [*key.namespace.values(), *key.name.values()]:
            if not value or sep in value or value.startswith(('_', '.')) or value.endswith('_') or '/' in value:
                raise ValueError(
                    f'key value {value!r} must be non-empty, not contain {sep!r} or "/", '
                    'not start with "_" or "." and not end with "_"'
                )
        return sep.join(key.namespace.values()), sep.join(key.name.values())

    @staticmethod
    def _check_case(name: str, existing_names: list[str]) -> None:
        """Raises ValueError if `name` differs only by case from one of `existing_names`."""
        for existing_name in existing_names:
            if existing_name != name and existing_name.lower() == name.lower():
                raise ValueError(f'{name!r} differs only by case from existing {existing_name!r}, names are case-insensitive')

    @staticmethod
    def _check_partition_values(partitions: list[Partition]) -> None:
        for partition in partitions:
            for value in partition:
                # a datetime is a date, but would be silently truncated to one
                if isinstance(value, datetime.datetime) or not isinstance(value, (str, int, datetime.date)):
                    raise TypeError(f'unsupported partition value {value!r}')

    @staticmethod
    def _partition_arrays(key: DatasetKey, data: pa.Table, partitions: list[Partition]) -> list[pa.Array]:
        """Returns each partition level's values, one per partition, typed like data's column,
        or as dates for a DatePartition level.

        Raises TypeError if a value doesn't fit the column's type.
        """
        try:
            return [
                pa.array(
                    [partition[i] for partition in partitions],
                    type=pa.date32() if isinstance(level, DatePartition) else data.schema.field(level).type,
                )
                for i, level in enumerate(key.partition_by)
            ]
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            raise TypeError(f'partition values do not match the types of data\'s partition columns: {e}') from e

    def _cast_ns_timestamps_to_us(self, data: pa.Table) -> pa.Table:
        """Casts data's nanosecond timestamp columns to microseconds, for table formats that store up to microseconds.

        The cast is lossless when the values are whole microseconds, e.g. data upcast from us.
        Otherwise it raises TypeError, unless the config allows timestamp precision loss,
        then it truncates them with a warning.
        """
        from pfeed.config import get_config

        for i, col in enumerate(data.schema):
            if pa.types.is_timestamp(col.type) and col.type.unit == 'ns':
                us_type = pa.timestamp('us', tz=col.type.tz)
                column = data.column(i)
                try:
                    us_column = column.cast(us_type)
                except pa.ArrowInvalid:
                    if not get_config().allow_timestamp_precision_loss:
                        raise TypeError(
                            f'column {col.name!r} has sub-microsecond timestamps, but {type(self).__name__} stores microseconds; '
                            + 'use an IO that stores nanoseconds (e.g. DuckLakeIO, ParquetIO), '
                            + 'or allow truncating them with pfeed.configure(allow_timestamp_precision_loss=True, persist=True)'
                        ) from None
                    us_column = column.cast(us_type, safe=False)
                    num_truncated = (pl.Series(column).dt.nanosecond() % 1000 != 0).sum()
                    warnings.warn(
                        f'{type(self).__name__} truncated {num_truncated} timestamps of column {col.name!r} from ns to us',
                        RuntimeWarning,
                        stacklevel=2,
                    )
                data = data.set_column(i, col.with_type(us_type), us_column)
        return data

    def _marker_partition_by(self, key: DatasetKey) -> list[str]:
        """Returns the columns to partition a table with marker rows by, see _add_marker_rows().

        Only the column levels, a DatePartition level is logical only (see the class docstring).
        """
        return [*(level for level in key.partition_by if not isinstance(level, DatePartition)), self.IS_METADATA_COLUMN]

    def _add_marker_rows(self, key: DatasetKey, data: pa.Table, partitions: dict[Partition, Metadata]) -> pa.Table:
        """Returns data plus one marker row per partition, so metadata is written in the same transaction as data.

        A marker row has the partition's values in the partition columns, its metadata (JSON) in
        METADATA_COLUMN, True in IS_METADATA_COLUMN and null elsewhere;
        data rows have METADATA_COLUMN null and IS_METADATA_COLUMN False.
        A partition exists if and only if it has a marker row (commit marker), even with no data rows.

        A DatePartition level's value is the day's midnight (UTC) in its column, so it's within the day's range.

        The table must be partitioned by _marker_partition_by(), so marker rows are in files of their own:
        replacing a partition's marker row then replaces a tiny file, instead of rewriting a file with its data,
        and reading the metadata only reads the marker files.

        Raises TypeError if a partition value doesn't fit its column.
        """
        # marker rows are null in the data columns, so every column has to be nullable
        schema = pa.schema([
            *(field.with_nullable(True) for field in data.schema),
            pa.field(self.METADATA_COLUMN, pa.string()),
            pa.field(self.IS_METADATA_COLUMN, pa.bool_()),
        ])
        partition_arrays = {
            col: values.cast(data.schema.field(col).type)  # a DatePartition's dates to midnight (UTC)
            for col, values in zip(key.partition_columns, self._partition_arrays(key, data, list(partitions)), strict=True)
        }
        markers = pa.table(
            [
                *(
                    partition_arrays[field.name] if field.name in partition_arrays else pa.nulls(len(partitions), field.type)
                    for field in data.schema
                ),
                pa.array([self._dump_metadata(md) for md in partitions.values()], pa.string()),
                pa.array([True] * len(partitions)),
            ],
            schema=schema,
        )
        data = (
            data.append_column(self.METADATA_COLUMN, pa.nulls(data.num_rows, pa.string()))
            .append_column(self.IS_METADATA_COLUMN, pa.array([False] * data.num_rows))
            .cast(schema)
        )
        return pa.concat_tables([data, markers])

    def _read_marker_rows(
        self, key: DatasetKey, lf: pl.LazyFrame, partitions: list[Partition] | None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        """Splits a table written with _add_marker_rows() into (data, metadata), as returned by read().

        `lf` must be pinned to one table version, so that data and metadata match.
        """
        schema = lf.collect_schema()

        def in_partition(partition: Partition) -> pl.Expr:
            predicates = []
            for level, value in zip(key.partition_by, partition, strict=True):
                if isinstance(level, DatePartition):
                    assert isinstance(value, datetime.date)
                    col, dtype = pl.col(level.column), schema[level.column]
                    start = datetime.datetime.combine(value, datetime.time(), tzinfo=datetime.UTC)
                    end = start + datetime.timedelta(days=1)
                    predicates.append((col >= pl.lit(start).cast(dtype)) & (col < pl.lit(end).cast(dtype)))
                else:
                    predicates.append(pl.col(level) == pl.lit(value))
            return reduce(operator.and_, predicates)

        if partitions is not None and key.partition_by:
            lf = lf.filter(reduce(operator.or_, [in_partition(partition) for partition in partitions]))
        # a DatePartition's marker row has the day's midnight (UTC), see _add_marker_rows()
        partition_values = [
            pl.col(level.column).cast(pl.Date) if isinstance(level, DatePartition) else pl.col(level)
            for level in key.partition_by
        ]
        markers = lf.filter(pl.col(self.IS_METADATA_COLUMN)).select(*partition_values, self.METADATA_COLUMN).collect()
        metadata: dict[Partition, Metadata] = {tuple(row[:-1]): json.loads(row[-1]) for row in markers.iter_rows()}
        if not metadata:
            return None, {}
        # no need to filter the data by the partitions with metadata (commit marker),
        # since a write adds a partition's data rows and its marker row in one transaction
        data = lf.filter(~pl.col(self.IS_METADATA_COLUMN)).drop(self.METADATA_COLUMN, self.IS_METADATA_COLUMN)
        if data.head(1).collect().is_empty():
            return None, metadata
        return data, metadata

    def write(
        self,
        key: DatasetKey,
        data: pa.Table,
        *,
        partitions: dict[Partition, Metadata],
        mode: Literal['replace', 'append'] = 'replace',
    ) -> None:
        """See BaseIO.write(). Append also requires metadata, which replaces the given partitions' metadata.

        data cannot have the columns METADATA_COLUMN and IS_METADATA_COLUMN, they are reserved.
        """
        self._data_partitions(key, data, partitions)
        if reserved := {self.METADATA_COLUMN, self.IS_METADATA_COLUMN} & set(data.column_names):
            raise ValueError(f'columns {reserved} are reserved for pfeed\'s metadata')
        if not partitions:
            return  # nothing to write; also, an empty filter would match every row of an unpartitioned dataset
        self._check_partition_values(list(partitions))
        self._write(key, data, partitions, mode)

    @abstractmethod
    def _write(
        self, key: DatasetKey, data: pa.Table, partitions: dict[Partition, Metadata], mode: Literal['replace', 'append'],
    ) -> None:
        """write() after validating its args; `partitions` is non-empty."""

    def read(
        self,
        key: DatasetKey,
        *,
        partitions: list[Partition] | None = None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        if partitions is not None:
            if not partitions:
                return None, {}
            self._check_partition_values(partitions)
        return self._read(key, partitions)

    @abstractmethod
    def _read(
        self, key: DatasetKey, partitions: list[Partition] | None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        """read() after validating its args; `partitions` is None or non-empty."""

    def _with_retries[T](self, transaction: Callable[[], T], description: str) -> T:
        """Runs the transaction, retrying it as a whole while it conflicts with a concurrent one."""
        attempt = 0
        while True:
            try:
                return transaction()
            except self._RETRY_ON as e:
                if attempt == self.MAX_RETRIES:
                    e.add_note(
                        f'{description} kept conflicting with concurrent writes, gave up after {self.MAX_RETRIES} retries'
                    )
                    raise
                # jitter so the conflicting writers don't retry in lockstep
                time.sleep(self.RETRY_WAIT * self.RETRY_BACKOFF**attempt * random.uniform(0.5, 1.5))
                attempt += 1

    @staticmethod
    def _check_retention(retention: datetime.timedelta) -> None:
        if retention < datetime.timedelta(0):
            raise ValueError(f'retention must not be negative, got {retention!r}')

    @abstractmethod
    def optimize(self) -> None:
        """Compacts every table, so reads scan fewer and larger parquet files.

        Only adds files: the replaced ones stay on disk, still used by old versions, until vacuum().
        """

    @abstractmethod
    def vacuum(
        self, *, retention: datetime.timedelta = DEFAULT_VACUUM_RETENTION, dry_run: bool = True,
    ) -> VacuumResult:
        """Deletes the history older than `retention`, run after optimize() so the files it replaced are deleted too.

        - expires the snapshots older than `retention` (where the format doesn't do it itself)
        - deletes files no longer used by the latest table version, once they stopped being used `retention` ago
        - deletes files no version ever used (e.g. left by a crashed write), once older than `retention`
        - time travel to versions older than `retention` is no longer possible

        A LazyFrame from read() must be collected within `retention`, or collecting it may fail.

        Args:
            retention: how long to keep history.
            dry_run: only returns what would be expired/deleted.

        Returns:
            the expired snapshots and deleted files, or what would be expired/deleted if dry_run.
        """
