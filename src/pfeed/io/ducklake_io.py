from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    import polars as pl

    from pfeed.io.base_io import DatasetKey, Metadata, Partition, PartitionValue

import contextlib
import datetime
import json
import os
import sqlite3

import duckdb
import pyarrow as pa

from pfeed.io.base_io import DatePartition
from pfeed.io.table_io import TableIO, VacuumResult


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class DuckLakeIO(TableIO):
    """Stores datasets as DuckLake tables, with a SQLite catalog; metadata in a per-dataset metadata table.

    See TableIO for what all table-format IOs share.
    Layout: <base_path>/pfeed.db (catalog) + <base_path>/data/ (parquet files written by DuckLake)
    - DatasetKey -> table "<schema>"."<table>", see TableIO.
    - metadata: table "<table>__metadata" in the same schema, one row per partition:
        the partition_by columns (a DatePartition's as a date) + METADATA_COLUMN (JSON string).
    - a DatePartition level partitions the data files by its column's year/month/day (UTC);
        its column can't be a nanosecond timestamp with a time zone, DuckLake can't partition it.
        Written in the same transaction as the data, so the commit marker comes for free.
    - replace/append are one transaction each: DELETE (replace only) + INSERT data, DELETE + INSERT metadata.
        Concurrent writes to the same dataset conflict even on disjoint partitions (DuckLake detects
        conflicts per table), so a conflicting transaction is retried as a whole.
    """

    DEFAULT_DIR_NAME: ClassVar[str] = 'ducklake'
    CATALOG_FILE_NAME: ClassVar[str] = 'pfeed.db'
    DATA_DIR_NAME: ClassVar[str] = 'data'
    METADATA_TABLE_SUFFIX: ClassVar[str] = '__metadata'
    _RETRY_ON: ClassVar[tuple[type[Exception], ...]] = (duckdb.TransactionException,)
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
        super().__init__(base_path)
        self._data_inlining_row_limit = data_inlining_row_limit
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
            # otherwise timestamps with a time zone are returned in the machine's local time zone;
            # GLOBAL, so the cursors (new connections) have it too
            conn.execute("SET GLOBAL TimeZone = 'UTC'")
            conn.execute(f"ATTACH 'ducklake:sqlite:{catalog_path}' AS {self._CATALOG_ALIAS} ({options})")
            self._conn, self._conn_pid = conn, os.getpid()
        return self._conn.cursor()

    def _dataset_tables(self, key: DatasetKey) -> tuple[str, str, str]:
        """Returns (schema, table, metadata table) of the dataset, see TableIO._table_names()."""
        schema, table = self._table_names(key)
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
    def _in_partitions(key: DatasetKey, partitions: list[Partition]) -> tuple[str, list[PartitionValue]]:
        """Returns a WHERE clause matching rows in `partitions`, with its query params.

        Works on both the data and the metadata table: a DatePartition level matches its column
        by the date's range [date, date + 1 day), which is the date itself in the metadata table,
        and lets DuckLake skip the data files of other days (a CAST(column AS DATE) filter wouldn't).
        """
        if not key.partition_by:
            return '', []  # one partition, every row is in it
        conditions: list[str] = []
        params: list[PartitionValue] = []
        for partition in partitions:
            level_conditions: list[str] = []
            for level, value in zip(key.partition_by, partition, strict=True):
                if isinstance(level, DatePartition):
                    if isinstance(value, datetime.datetime) or not isinstance(value, datetime.date):
                        raise TypeError(f'{level} partition value must be a date, got {value!r}')
                    level_conditions.append(f'{_quote(level.column)} >= ? AND {_quote(level.column)} < ?')
                    params += [value, value + datetime.timedelta(days=1)]
                else:
                    level_conditions.append(f'{_quote(level)} = ?')
                    params.append(value)
            conditions.append('(' + ' AND '.join(level_conditions) + ')')
        return 'WHERE ' + ' OR '.join(conditions), params

    @staticmethod
    def _partitioned_by(key: DatasetKey) -> str:
        """Returns the SET PARTITIONED BY expressions of the data table, a DatePartition level being its column's
        year/month/day, so each day's rows are in files of their own, e.g. product=BTC/year=2025/month=1/day=1/.
        """
        exprs: list[str] = []
        for level in key.partition_by:
            if isinstance(level, DatePartition):
                col = _quote(level.column)
                exprs += [f'year({col})', f'month({col})', f'day({col})']
            else:
                exprs.append(_quote(level))
        return ', '.join(exprs)

    @staticmethod
    def _check_date_partition_types(key: DatasetKey, data: pa.Table) -> None:
        """Raises TypeError if a DatePartition column is a nanosecond timestamp with a time zone,
        since DuckLake can't compute its year/month/day (no year(TIMESTAMPTZ_NS)).
        """
        for level in key.partition_by:
            if isinstance(level, DatePartition):
                dtype = data.schema.field(level.column).type
                if pa.types.is_timestamp(dtype) and dtype.tz is not None and dtype.unit == 'ns':
                    raise TypeError(
                        f'{level} column {level.column!r} is {dtype}, DuckLake can\'t partition nanosecond timestamps '
                        'with a time zone; drop the time zone (UTC) or use microseconds'
                    )

    def _write(
        self, key: DatasetKey, data: pa.Table, partitions: dict[Partition, Metadata], mode: Literal['replace', 'append'],
    ) -> None:
        self._check_date_partition_types(key, data)
        # one row per partition: its values (typed like data's partition columns, a DatePartition's as dates,
        # so the filters match both tables) + metadata
        partition_metadata_table = pa.table({
            **dict(zip(key.partition_columns, self._partition_arrays(key, data, list(partitions)), strict=True)),
            self.METADATA_COLUMN: [self._dump_metadata(md) for md in partitions.values()],
        })

        self._with_retries(
            lambda: self._write_transaction(key, data, list(partitions), partition_metadata_table, mode), f'write to {key}'
        )

    def _write_transaction(
        self, key: DatasetKey, data: pa.Table, partitions: list[Partition], partition_metadata_table: pa.Table, mode: str,
    ) -> None:
        schema, table, metadata_table = self._dataset_tables(key)
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
                    cursor.execute(f'ALTER TABLE {data_ref} SET PARTITIONED BY ({self._partitioned_by(key)})')
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

    def _read(
        self, key: DatasetKey, partitions: list[Partition] | None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        schema, table, metadata_table = self._dataset_tables(key)
        data_ref = f'{self._CATALOG_ALIAS}.{_quote(schema)}.{_quote(table)}'
        metadata_ref = f'{self._CATALOG_ALIAS}.{_quote(schema)}.{_quote(metadata_table)}'
        cursor = self._connect()
        # metadata and snapshot id from the same transaction, so the data can be pinned to that snapshot
        cursor.execute('BEGIN')
        try:
            if not self._table_exists(cursor, schema, table):
                return None, {}
            ((snapshot_id,),) = cursor.execute(f'SELECT id FROM {self._CATALOG_ALIAS}.current_snapshot()').fetchall()
            predicate, params = self._in_partitions(key, partitions) if partitions is not None else ('', [])
            cols = ', '.join([*(_quote(col) for col in key.partition_columns), self.METADATA_COLUMN])
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
        """See TableIO.optimize().

        - moves inlined rows (see data_inlining_row_limit) out of the catalog into parquet files
        - rewrites files where most rows were deleted (e.g. by 'replace' writes)
        - merges small files into larger ones
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
        self, *, retention: datetime.timedelta = TableIO.DEFAULT_VACUUM_RETENTION, dry_run: bool = True,
    ) -> VacuumResult:
        """See TableIO.vacuum().

        - expires snapshots older than `retention` (the latest snapshot is never expired)
        - deletes files that expired snapshots no longer use, `retention` after they were expired,
            so a file is only deleted by a later vacuum than the one expiring its snapshot
        - deletes files no snapshot ever used (e.g. left by a crashed write), once older than `retention`
        """
        self._check_retention(retention)
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
        return VacuumResult(
            expired_snapshots=[str(snapshot_id) for (snapshot_id,) in snapshot_ids],
            deleted_paths=[path for (path,) in paths],
        )
