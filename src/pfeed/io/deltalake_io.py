# NOTE: NOT supported yet (not exported, no CLI command, no pip extra).
# Only implemented and tested alongside DuckLakeIO to derive a good TableIO foundation for it.
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from pfeed.io.base_io import DatasetKey, Metadata, Partition, PartitionValue

import datetime
import math
import os

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, Schema, write_deltalake
from deltalake.exceptions import CommitFailedError

from pfeed.io.table_io import TableIO, VacuumResult


def _sql_literal(value: PartitionValue) -> str:
    if isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, int):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


class DeltaLakeIO(TableIO):
    """Stores datasets as Delta Lake tables; metadata as marker rows in the same table.

    See TableIO for what all table-format IOs share.
    Layout: <base_path>/<schema>/<table>/ is one Delta table (with its _delta_log/), see TableIO for the names.
    - metadata: one marker row per partition in the data table itself, see TableIO._add_marker_rows(),
        since a Delta transaction can't span two tables.
    - replace: one overwrite of the partitions (data + marker rows), dropping their old files.
    - append: one merge, inserting the data rows and replacing the partitions' marker rows;
        only the marker files of these partitions are scanned and rewritten.
    - delta-rs rebases a commit onto concurrent ones if they don't conflict, so writes to disjoint
        partitions mostly just land; a commit that still conflicts is retried as a whole.
    """

    DEFAULT_DIR_NAME: ClassVar[str] = 'deltalake'
    _RETRY_ON: ClassVar[tuple[type[Exception], ...]] = (CommitFailedError,)
    _LOG_DIR_NAME: ClassVar[str] = '_delta_log'

    def _table_path(self, key: DatasetKey) -> str:
        """Returns the table's directory, raising ValueError if its schema/table dir exists under a different case.

        Checked even on case-sensitive filesystems, so the same keys work everywhere.
        """
        schema, table = self._table_names(key)
        schema_dir = os.path.join(self._base_path, schema)
        if os.path.isdir(self._base_path):
            self._check_case(schema, os.listdir(self._base_path))
        if os.path.isdir(schema_dir):
            self._check_case(table, os.listdir(schema_dir))
        return os.path.join(schema_dir, table)

    def _load_table(self, path: str) -> DeltaTable | None:
        # not DeltaTable.is_deltatable(), which creates the directory if it doesn't exist
        if not os.path.isdir(os.path.join(path, self._LOG_DIR_NAME)):
            return None
        return DeltaTable(path)

    def _table_paths(self) -> list[str]:
        """Returns the directories of all tables under base_path."""
        if not os.path.isdir(self._base_path):
            return []
        return sorted(
            entry.path
            for schema_entry in os.scandir(self._base_path) if schema_entry.is_dir()
            for entry in os.scandir(schema_entry.path)
            if os.path.isdir(os.path.join(entry.path, self._LOG_DIR_NAME))
        )

    @staticmethod
    def _sql_predicate(key: DatasetKey, partitions: list[Partition], alias: str = '') -> str | None:
        """Returns a delta-rs SQL predicate matching rows in `partitions`, None for an unpartitioned dataset."""
        if not key.partition_by:
            return None  # one partition, every row is in it
        prefix = f'{alias}.' if alias else ''
        return ' OR '.join(
            '(' + ' AND '.join(
                f'{prefix}"{col}" = {_sql_literal(value)}' for col, value in zip(key.partition_by, partition, strict=True)
            ) + ')'
            for partition in partitions
        )

    @staticmethod
    def _check_types(dt: DeltaTable, table: pa.Table) -> None:
        """Raises TypeError if a column of `table` has a different type than in the Delta table.

        Compared as Delta types, so e.g. string and large_string are the same.
        """
        existing_types = {field.name: field.type for field in dt.schema().fields}
        for field in Schema.from_arrow(table.schema).fields:
            if field.name in existing_types and field.type != existing_types[field.name]:
                raise TypeError(f'column {field.name!r} is {field.type}, but {existing_types[field.name]} in the dataset')

    def _write(
        self, key: DatasetKey, data: pa.Table, partitions: dict[Partition, Metadata], mode: Literal['replace', 'append'],
    ) -> None:
        # Delta Lake stores timestamps in microseconds, delta-rs would silently truncate ns ones
        data = self._cast_ns_timestamps_to_us(data)
        table = self._add_marker_rows(key, data, partitions)
        path = self._table_path(key)
        self._with_retries(lambda: self._write_transaction(key, path, table, list(partitions), mode), f'write to {key}')

    def _write_transaction(
        self, key: DatasetKey, path: str, table: pa.Table, partitions: list[Partition], mode: str,
    ) -> None:
        dt = self._load_table(path)
        if dt is not None:
            self._check_types(dt, table)
        partition_by = self._marker_partition_by(key)
        if dt is None or mode == 'replace':
            # on a new table, 'overwrite' just creates it; schema drift: 'merge' adds new columns, missing ones are null
            write_deltalake(
                path, table, mode='overwrite', predicate=self._sql_predicate(key, partitions),
                partition_by=partition_by, schema_mode='merge',
            )
            return
        # match a source marker row with its partition's existing marker row, so it replaces it;
        # everything else (data rows, markers of new partitions) is inserted
        is_meta = self.IS_METADATA_COLUMN
        # the literal conditions on the target's partition columns let the merge skip all other files
        predicate = ' AND '.join([
            *(f'({target})' for target in [self._sql_predicate(key, partitions, alias='t')] if target),
            f't."{is_meta}" = true',
            f's."{is_meta}" = true',
            *(f't."{col}" = s."{col}"' for col in key.partition_by),
        ])
        (
            dt.merge(table, predicate=predicate, source_alias='s', target_alias='t', merge_schema=True)
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    def _read(
        self, key: DatasetKey, partitions: list[Partition] | None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        dt = self._load_table(self._table_path(key))
        if dt is None:
            return None, {}
        # scans the version dt was loaded at, so data and metadata match
        return self._read_marker_rows(key, pl.scan_delta(dt), partitions)

    def optimize(self) -> None:
        """See TableIO.optimize(). Merges small files into larger ones, per partition."""
        for path in self._table_paths():
            self._with_retries(lambda path=path: DeltaTable(path).optimize.compact(), f'optimize {path}')

    def vacuum(
        self, *, retention: datetime.timedelta = TableIO.DEFAULT_VACUUM_RETENTION, dry_run: bool = True,
    ) -> VacuumResult:
        """See TableIO.vacuum().

        Delta Lake only takes whole hours, so `retention` is rounded up to them.
        Expires no versions: Delta Lake cleans up old log entries (time travel) itself, see delta.logRetentionDuration.
        """
        self._check_retention(retention)
        retention_hours = math.ceil(retention / datetime.timedelta(hours=1))
        paths = []
        for path in self._table_paths():
            # full: also deletes files the log never referenced, e.g. left by a crashed write
            deleted = self._with_retries(
                lambda path=path: DeltaTable(path).vacuum(
                    retention_hours=retention_hours, dry_run=dry_run, enforce_retention_duration=False, full=True,
                ),
                f'vacuum {path}',
            )
            paths += [os.path.join(path, file) for file in deleted]
        return VacuumResult(deleted_paths=paths)
