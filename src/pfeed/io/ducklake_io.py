from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

    import polars as pl

    from pfeed.io.base_io import DatasetKey, Metadata, Partition, PartitionValue

import contextlib
import datetime
import json
import os
import random
import sqlite3
import time
from urllib.parse import urlparse

import duckdb
import pyarrow as pa

from pfeed.io.base_io import BaseIO, IOCapabilities


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class DuckLakeIO(BaseIO):
    """Stores datasets as DuckLake tables, with a SQLite catalog; metadata in a per-dataset metadata table.

    Layout: <base_path>/pfeed.db (catalog) + <base_path>/data/ (parquet files written by DuckLake)
    - DatasetKey -> table: schema = namespace values joined by NAME_SEPARATOR, table = name values joined by it,
        e.g. "BACKTEST__BYBIT"."PERPETUAL__1t".
        DuckDB names are case-insensitive, so a key differing from an existing dataset only by case raises.
    - metadata: table "<table>__metadata" in the same schema, one row per partition:
        the partition_by columns + METADATA_COLUMN (JSON string).
        Written in the same transaction as the data, so the commit marker comes for free.
    - replace/append are one transaction each: DELETE (replace only) + INSERT data, DELETE + INSERT metadata.
        Concurrent writes to the same dataset conflict even on disjoint partitions (DuckLake detects
        conflicts per table), so a conflicting transaction is retried as a whole.
    - read pins the data to the snapshot the metadata was read at, so they always match.
    - nothing is maintained automatically: run optimize() to compact the lake (e.g. after many small writes),
        then vacuum() to delete what old snapshots no longer need.
    """

    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities(append=True, concurrent_partition_writes=True)
    DEFAULT_DIR_NAME: ClassVar[str] = 'ducklake'
    CATALOG_FILE_NAME: ClassVar[str] = 'pfeed.db'
    DATA_DIR_NAME: ClassVar[str] = 'data'
    # '__' instead of '_', since key values like 'market_data' contain '_'
    NAME_SEPARATOR: ClassVar[str] = '__'
    METADATA_TABLE_SUFFIX: ClassVar[str] = '__metadata'
    METADATA_COLUMN: ClassVar[str] = 'pfeed_metadata'
    # same as Delta Lake's default
    DEFAULT_VACUUM_RETENTION: ClassVar[datetime.timedelta] = datetime.timedelta(days=7)
    # retries of a write/maintenance transaction that conflicted with a concurrent one,
    # waiting RETRY_WAIT * RETRY_BACKOFF**attempt (with jitter) in between
    MAX_RETRIES: ClassVar[int] = 10
    RETRY_WAIT: ClassVar[float] = 0.1
    RETRY_BACKOFF: ClassVar[float] = 1.5
    _CATALOG_ALIAS: ClassVar[str] = 'lake'

    def __init__(self, base_path: str | None = None, data_inlining_row_limit: int | None = None):
        """
        Args:
            base_path: root directory of the catalog and data files. Local paths only for now.
                Defaults to <config.data_path>/<DEFAULT_DIR_NAME>.
            data_inlining_row_limit: inserts of up to this many rows are stored inside the catalog
                instead of as parquet files, so small writes (e.g. live appends) don't create many tiny files.
                0 disables inlining; None uses DuckLake's default.
        """
        if data_inlining_row_limit is not None and (
            not isinstance(data_inlining_row_limit, int) or data_inlining_row_limit < 0
        ):
            raise ValueError(f'data_inlining_row_limit must be an int >= 0, got {data_inlining_row_limit!r}')
        self._data_inlining_row_limit = data_inlining_row_limit
        if base_path is None:
            from pfeed.config import get_config

            base_path = str(get_config().data_path / self.DEFAULT_DIR_NAME)
        if urlparse(base_path).scheme not in ('', 'file'):
            raise NotImplementedError(f'only local paths are supported for now, got {base_path!r}')
        self._base_path = os.path.abspath(base_path.removeprefix('file://'))
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._conn_pid: int | None = None
        # creates the catalog now, since processes creating it at the same time fail with "database is locked"
        self._connect().close()
        # WAL keeps catalog reads from waiting on a concurrent write (in 1s busy waits),
        # which made concurrent writes ~5x slower and sometimes fail with "database is locked"
        with contextlib.closing(sqlite3.connect(os.path.join(self._base_path, self.CATALOG_FILE_NAME))) as conn:
            conn.execute('PRAGMA journal_mode=WAL')

    def __getstate__(self) -> dict:
        # a connection can't be pickled (e.g. sent to a Ray worker), each process opens its own
        return self.__dict__ | {'_conn': None, '_conn_pid': None}

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Returns a new cursor on this process's connection, attaching the catalog on first use.

        A cursor per call keeps threads from sharing one, and a connection inherited by fork isn't reused.
        """
        if self._conn is None or self._conn_pid != os.getpid():
            os.makedirs(self._base_path, exist_ok=True)
            catalog_path = os.path.join(self._base_path, self.CATALOG_FILE_NAME).replace("'", "''")
            data_path = os.path.join(self._base_path, self.DATA_DIR_NAME, '').replace("'", "''")
            options = f"DATA_PATH '{data_path}'"
            # not stored in the catalog, so it has to be passed on every attach
            if self._data_inlining_row_limit is not None:
                options += f', DATA_INLINING_ROW_LIMIT {self._data_inlining_row_limit}'
            conn = duckdb.connect()
            conn.execute(f"ATTACH 'ducklake:sqlite:{catalog_path}' AS {self._CATALOG_ALIAS} ({options})")
            self._conn, self._conn_pid = conn, os.getpid()
        return self._conn.cursor()

    def _table_names(self, key: DatasetKey) -> tuple[str, str, str]:
        """Returns (schema, table, metadata table) of the dataset.

        Raises ValueError if a key value could make two different keys join to the same name,
        e.g. ('A_', 'B') and ('A', '_B') would both join to 'A___B'.
        """
        sep = self.NAME_SEPARATOR
        for value in [*key.namespace.values(), *key.name.values()]:
            if not value or sep in value or value.startswith('_') or value.endswith('_'):
                raise ValueError(
                    f'key value {value!r} must be non-empty, not contain {sep!r} and not start or end with "_"'
                )
        schema, table = sep.join(key.namespace.values()), sep.join(key.name.values())
        if table.endswith(self.METADATA_TABLE_SUFFIX):
            raise ValueError(f'dataset name {table!r} cannot end with {self.METADATA_TABLE_SUFFIX!r}, it is reserved')
        return schema, table, table + self.METADATA_TABLE_SUFFIX

    def _table_exists(self, cursor: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
        """Checks if the table exists, raising ValueError if its schema/table exists under a different case."""
        found = cursor.execute(
            """
            SELECT s.schema_name, t.table_name
            FROM duckdb_schemas() s
            LEFT JOIN duckdb_tables() t
                ON t.database_name = s.database_name AND t.schema_name = s.schema_name
                AND lower(t.table_name) = lower(?)
            WHERE s.database_name = ? AND lower(s.schema_name) = lower(?)
            """,
            [table, self._CATALOG_ALIAS, schema],
        ).fetchall()
        for found_schema, found_table in found:
            if found_schema != schema or (found_table is not None and found_table != table):
                raise ValueError(
                    f'{schema}.{table} differs only by case from existing {found_schema}.{found_table}, '
                    'DuckDB names are case-insensitive'
                )
        return any(found_table is not None for _, found_table in found)

    @staticmethod
    def _check_partition_values(partitions: list[Partition]) -> None:
        for partition in partitions:
            for value in partition:
                # a datetime is a date, but would be silently truncated to one
                if isinstance(value, datetime.datetime) or not isinstance(value, (str, int, datetime.date)):
                    raise TypeError(f'unsupported partition value {value!r}')

    @staticmethod
    def _in_partitions(key: DatasetKey, partitions: list[Partition]) -> tuple[str, list[PartitionValue]]:
        """Returns a WHERE clause matching rows in `partitions`, with its query params."""
        if not key.partition_by:
            return '', []  # one partition, every row is in it
        cols = ', '.join(_quote(col) for col in key.partition_by)
        placeholders = ', '.join('(' + ', '.join(['?'] * len(key.partition_by)) + ')' for _ in partitions)
        return f'WHERE ({cols}) IN (VALUES {placeholders})', [value for partition in partitions for value in partition]

    def write(
        self,
        key: DatasetKey,
        data: pa.Table,
        *,
        partitions: dict[Partition, Metadata],
        mode: Literal['replace', 'append'] = 'replace',
    ) -> None:
        """See BaseIO.write(). Append also requires metadata, which replaces the given partitions' metadata."""
        self._data_partitions(key, data, partitions)
        if not partitions:
            return  # nothing to write; also, an empty predicate would match every row of an unpartitioned dataset
        self._check_partition_values(list(partitions))
        # one row per partition: its values (typed like data's partition columns, so the IN filters match both tables) + metadata
        try:
            partition_metadata_table = pa.table({
                **{
                    col: pa.array([partition[i] for partition in partitions], type=data.schema.field(col).type)
                    for i, col in enumerate(key.partition_by)
                },
                self.METADATA_COLUMN: [self._dump_metadata(md) for md in partitions.values()],
            })
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            raise TypeError(f'partition values do not match the types of data\'s partition columns: {e}') from e

        self._with_retries(
            lambda: self._write_transaction(key, data, list(partitions), partition_metadata_table, mode), f'write to {key}'
        )

    def _with_retries[T](self, transaction: Callable[[], T], description: str) -> T:
        """Runs the transaction, retrying it as a whole while it conflicts with a concurrent one."""
        attempt = 0
        while True:
            try:
                return transaction()
            except duckdb.TransactionException as e:
                if attempt == self.MAX_RETRIES:
                    raise duckdb.TransactionException(
                        f'{description} kept conflicting with concurrent writes, gave up after {self.MAX_RETRIES} retries'
                    ) from e
                # jitter so the conflicting writers don't retry in lockstep
                time.sleep(self.RETRY_WAIT * self.RETRY_BACKOFF**attempt * random.uniform(0.5, 1.5))
                attempt += 1

    def _write_transaction(
        self, key: DatasetKey, data: pa.Table, partitions: list[Partition], partition_metadata_table: pa.Table, mode: str,
    ) -> None:
        schema, table, metadata_table = self._table_names(key)
        data_ref = f'{self._CATALOG_ALIAS}.{_quote(schema)}.{_quote(table)}'
        metadata_ref = f'{self._CATALOG_ALIAS}.{_quote(schema)}.{_quote(metadata_table)}'
        cursor = self._connect()
        cursor.register('_data', data)
        cursor.register('_partition_metadata', partition_metadata_table)
        cursor.execute('BEGIN')
        try:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {self._CATALOG_ALIAS}.{_quote(schema)}')
            if not self._table_exists(cursor, schema, table):
                cursor.execute(f'CREATE TABLE {data_ref} AS SELECT * FROM _data LIMIT 0')
                if key.partition_by:
                    cols = ', '.join(_quote(col) for col in key.partition_by)
                    cursor.execute(f'ALTER TABLE {data_ref} SET PARTITIONED BY ({cols})')
                cursor.execute(f'CREATE TABLE {metadata_ref} AS SELECT * FROM _partition_metadata LIMIT 0')
            else:
                # schema drift: add new columns, missing ones are null-filled by INSERT BY NAME
                existing_types = {row[0]: row[1] for row in cursor.execute(f'DESCRIBE {data_ref}').fetchall()}
                for col, dtype, *_ in cursor.execute('DESCRIBE SELECT * FROM _data').fetchall():
                    if col not in existing_types:
                        cursor.execute(f'ALTER TABLE {data_ref} ADD COLUMN {_quote(col)} {dtype}')
                    elif existing_types[col] != dtype:
                        raise TypeError(f'column {col!r} is {dtype}, but {existing_types[col]} in the dataset')
            predicate, params = self._in_partitions(key, partitions)
            if mode == 'replace':
                cursor.execute(f'DELETE FROM {data_ref} {predicate}', params)
            cursor.execute(f'INSERT INTO {data_ref} BY NAME SELECT * FROM _data')
            cursor.execute(f'DELETE FROM {metadata_ref} {predicate}', params)
            cursor.execute(f'INSERT INTO {metadata_ref} BY NAME SELECT * FROM _partition_metadata')
            cursor.execute('COMMIT')
        except BaseException:
            # a failed COMMIT has already rolled back, then ROLLBACK raises and would hide the original error
            with contextlib.suppress(duckdb.TransactionException):
                cursor.execute('ROLLBACK')
            raise
        finally:
            cursor.close()

    def read(
        self,
        key: DatasetKey,
        *,
        partitions: list[Partition] | None = None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        schema, table, metadata_table = self._table_names(key)
        data_ref = f'{self._CATALOG_ALIAS}.{_quote(schema)}.{_quote(table)}'
        metadata_ref = f'{self._CATALOG_ALIAS}.{_quote(schema)}.{_quote(metadata_table)}'
        if partitions is not None:
            if not partitions:
                return None, {}
            self._check_partition_values(partitions)

        cursor = self._connect()
        # metadata and snapshot id from the same transaction, so the data can be pinned to that snapshot
        cursor.execute('BEGIN')
        try:
            if not self._table_exists(cursor, schema, table):
                return None, {}
            ((snapshot_id,),) = cursor.execute(f'SELECT id FROM {self._CATALOG_ALIAS}.current_snapshot()').fetchall()
            predicate, params = self._in_partitions(key, partitions) if partitions is not None else ('', [])
            cols = ', '.join([*(_quote(col) for col in key.partition_by), self.METADATA_COLUMN])
            rows = cursor.execute(f'SELECT {cols} FROM {metadata_ref} {predicate}', params).fetchall()
        finally:
            cursor.execute('COMMIT')
        metadata: dict[Partition, Metadata] = {tuple(row[:-1]): json.loads(row[-1]) for row in rows}
        if not metadata:
            return None, {}

        # only rows of the partitions with metadata (commit marker)
        predicate, params = self._in_partitions(key, list(metadata))
        relation = cursor.sql(f'SELECT * FROM {data_ref} AT (VERSION => {snapshot_id}) {predicate}', params=params)
        if relation.limit(1).fetchone() is None:
            return None, metadata
        return relation.pl(lazy=True), metadata

    def optimize(self) -> None:
        """Compacts every table, so reads scan fewer and larger parquet files.

        - moves inlined rows (see data_inlining_row_limit) out of the catalog into parquet files
        - rewrites files where most rows were deleted (e.g. by 'replace' writes)
        - merges small files into larger ones
        Only adds files: the replaced ones stay on disk, still used by old snapshots, until vacuum().
        """
        cursor = self._connect()
        try:
            for function in ('ducklake_flush_inlined_data', 'ducklake_rewrite_data_files', 'ducklake_merge_adjacent_files'):
                self._with_retries(
                    lambda function=function: cursor.execute(f'CALL {function}(?)', [self._CATALOG_ALIAS]),
                    function,
                )
        finally:
            cursor.close()

    def vacuum(
        self, *, retention: datetime.timedelta = DEFAULT_VACUUM_RETENTION, dry_run: bool = True,
    ) -> tuple[list[int], list[str]]:
        """Deletes what is older than `retention`, run after optimize() so the files it replaced are deleted too.

        - expires snapshots older than `retention` (the latest snapshot is never expired)
        - deletes files that expired snapshots no longer use, `retention` after they were expired
        - deletes files no snapshot ever used (e.g. left by a crashed write), once older than `retention`

        Files are deleted no earlier than `retention` after the vacuum that expired their snapshot,
        so a LazyFrame from read() must be collected within that window, or collecting it fails.

        Args:
            retention: how long to keep history; time travel to a snapshot older than it is no longer possible.
            dry_run: only returns what would be expired/deleted.

        Returns:
            (expired snapshot ids, deleted file paths), or what would be expired/deleted if dry_run.
        """
        if retention < datetime.timedelta(0):
            raise ValueError(f'retention must not be negative, got {retention!r}')
        older_than = datetime.datetime.now(datetime.UTC) - retention
        params = [self._CATALOG_ALIAS, older_than, dry_run]
        cursor = self._connect()
        try:
            snapshot_ids = self._with_retries(
                lambda: cursor.execute(
                    'SELECT snapshot_id FROM ducklake_expire_snapshots(?, older_than => ?, dry_run => ?)', params
                ).fetchall(),
                'ducklake_expire_snapshots',
            )
            paths = []
            for function in ('ducklake_cleanup_old_files', 'ducklake_delete_orphaned_files'):
                paths += self._with_retries(
                    lambda function=function: cursor.execute(
                        f'SELECT path FROM {function}(?, older_than => ?, dry_run => ?)', params
                    ).fetchall(),
                    function,
                )
        finally:
            cursor.close()
        return [snapshot_id for (snapshot_id,) in snapshot_ids], [path for (path,) in paths]
