# NOTE: NOT supported yet (not exported, no CLI command, no pip extra).
# Only implemented and tested alongside DuckLakeIO to derive a good TableIO foundation for it.
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.table import Table

    from pfeed.io.base_io import DatasetKey, Metadata, Partition, PartitionValue

import contextlib
import datetime
import itertools
import os
import sqlite3
import warnings
from functools import reduce

import polars as pl
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
    ValidationError,
    ValidationException,
)
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, Or
from pyiceberg.io.pyarrow import UnsupportedPyArrowTypeException
from pyiceberg.table import TableProperties
from sqlalchemy.exc import IntegrityError, OperationalError

from pfeed.io.table_io import TableIO, VacuumResult


def _partition_filter(cols: Sequence[str], values: Sequence[PartitionValue]) -> BooleanExpression:
    """Returns a filter matching rows whose `cols` equal `values`; AlwaysTrue() if there are no cols."""
    # ty misreads the constructors of pyiceberg's (pydantic) expressions
    predicates = [EqualTo(col, value) for col, value in zip(cols, values, strict=True)]  # ty: ignore[missing-argument, too-many-positional-arguments]
    return reduce(And, predicates) if predicates else AlwaysTrue()


class IcebergIO(TableIO):
    """Stores datasets as Iceberg tables, with a SQLite catalog; metadata as marker rows in the same table.

    See TableIO for what all table-format IOs share.
    Layout: <base_path>/pfeed.db (catalog) + <base_path>/warehouse/<schema>/<table>/ (data/ and metadata/ files)
    - DatasetKey -> table "<schema>"."<table>" (namespace.table in Iceberg), see TableIO.
    - metadata: one marker row per partition in the data table itself, see TableIO._add_marker_rows(),
        since an Iceberg transaction (in pyiceberg) can't span two tables.
    - replace: one transaction overwriting the partitions (data + marker rows), dropping their old files.
    - append: one transaction dropping the partitions' marker files and appending data + new marker rows.
    - every concurrent commit conflicts in Iceberg, pyiceberg retries it by re-applying the transaction's files
        onto the latest version, after validating that the concurrent commits didn't touch the same partitions
        (e.g. a compaction's rewrite of a partition that was just written to); if they did, or pyiceberg ran out
        of retries, the transaction is retried as a whole.
    - pyiceberg can't compact or delete unused files, so optimize() and vacuum() do it themselves.
    """

    DEFAULT_DIR_NAME: ClassVar[str] = 'iceberg'
    CATALOG_FILE_NAME: ClassVar[str] = 'pfeed.db'
    WAREHOUSE_DIR_NAME: ClassVar[str] = 'warehouse'
    # ValidationException: a concurrent commit touched the same partitions;
    # OperationalError: "database is locked" from the SQLite catalog
    _RETRY_ON: ClassVar[tuple[type[Exception], ...]] = (CommitFailedException, ValidationException, OperationalError)
    _TABLE_PROPERTIES: ClassVar[dict[str, str]] = {
        # merges the manifests of small appends, instead of adding one manifest per write
        TableProperties.MANIFEST_MERGE_ENABLED: 'true',
    }

    def __init__(self, base_path: str | None = None):
        """
        Args:
            base_path: root directory of the catalog and warehouse. Local paths only for now.
                Defaults to <config.data_path>/<DEFAULT_DIR_NAME>.
        """
        super().__init__(base_path)
        self._catalog: SqlCatalog | None = None
        self._catalog_pid: int | None = None
        # creates the catalog now, since processes creating it at the same time fail with "database is locked"
        self._load_catalog()
        # WAL keeps catalog reads from waiting on a concurrent write
        with contextlib.closing(sqlite3.connect(os.path.join(self._base_path, self.CATALOG_FILE_NAME))) as conn:
            conn.execute('PRAGMA journal_mode=WAL')

    def __getstate__(self) -> dict:
        # a catalog (SQLAlchemy engine) can't be pickled (e.g. sent to a Ray worker), each process opens its own
        return self.__dict__ | {'_catalog': None, '_catalog_pid': None}

    def _load_catalog(self) -> SqlCatalog:
        """Returns this process's catalog, so a connection pool inherited by fork isn't reused."""
        if self._catalog is None or self._catalog_pid != os.getpid():
            os.makedirs(self._base_path, exist_ok=True)
            self._catalog = SqlCatalog(
                'pfeed',
                uri=f'sqlite:///{os.path.join(self._base_path, self.CATALOG_FILE_NAME)}',
                warehouse=f'file://{os.path.join(self._base_path, self.WAREHOUSE_DIR_NAME)}',
            )
            self._catalog_pid = os.getpid()
        return self._catalog

    def _identifier(self, catalog: SqlCatalog, key: DatasetKey) -> tuple[str, str]:
        """Returns (schema, table), raising ValueError if they exist under a different case.

        The catalog itself is case-sensitive, but tables are stored in <schema>/<table>/ directories,
        which aren't on e.g. macOS.
        """
        schema, table = self._table_names(key)
        namespaces = [namespace for (namespace, *_) in catalog.list_namespaces()]
        self._check_case(schema, namespaces)
        if schema in namespaces:
            self._check_case(table, [name for (_, name) in catalog.list_tables(schema)])
        return schema, table

    def _load_table(self, catalog: SqlCatalog, identifier: tuple[str, str]) -> Table | None:
        try:
            return catalog.load_table(identifier)
        except (NoSuchTableError, NoSuchNamespaceError):
            return None

    def _create_table(self, catalog: SqlCatalog, identifier: tuple[str, str], key: DatasetKey, schema: pa.Schema) -> Table:
        """Creates the table partitioned by _marker_partition_by(), or loads it if a concurrent write just created it."""
        # a namespace created concurrently fails the existence check, then the insert with IntegrityError
        with contextlib.suppress(NamespaceAlreadyExistsError, IntegrityError):
            catalog.create_namespace_if_not_exists(identifier[0])
        try:
            with (
                catalog.create_table_transaction(identifier, schema=schema, properties=self._TABLE_PROPERTIES) as tx,
                tx.update_spec() as update,
            ):
                for col in self._marker_partition_by(key):
                    update.add_identity(col)
        except (TableAlreadyExistsError, IntegrityError):
            pass
        return catalog.load_table(identifier)

    @staticmethod
    def _filter(key: DatasetKey, partitions: list[Partition]) -> BooleanExpression:
        """Returns a filter matching rows in `partitions`."""
        if not key.partition_by:
            return AlwaysTrue()  # one partition, every row is in it
        return reduce(Or, [_partition_filter(key.partition_by, partition) for partition in partitions])

    def _write(
        self, key: DatasetKey, data: pa.Table, partitions: dict[Partition, Metadata], mode: Literal['replace', 'append'],
    ) -> None:
        table = self._add_marker_rows(key, data, partitions)
        try:
            self._with_retries(lambda: self._write_transaction(key, table, list(partitions), mode), f'write to {key}')
        except UnsupportedPyArrowTypeException as e:
            # e.g. timestamp[ns], which only Iceberg v3 supports, and pyiceberg can't write v3 yet
            raise TypeError(str(e)) from e

    def _write_transaction(self, key: DatasetKey, table: pa.Table, partitions: list[Partition], mode: str) -> None:
        catalog = self._load_catalog()
        identifier = self._identifier(catalog, key)
        tbl = self._load_table(catalog, identifier) or self._create_table(catalog, identifier, key, table.schema)
        existing_types = {field.name: field.field_type for field in tbl.schema().fields}
        partition_filter = self._filter(key, partitions)
        # an exception inside the transaction leaves it uncommitted
        with tbl.transaction() as tx, warnings.catch_warnings():
            # e.g. "Delete operation did not match any records" when a partition is new
            warnings.simplefilter('ignore', UserWarning)
            # schema drift: add new columns, missing ones are null
            try:
                with tx.update_schema() as update:
                    update.union_by_name(table.schema)
            except ValidationError as e:
                raise TypeError(f'a column has a different type than in the dataset: {e}') from e
            for field in tx.table_metadata.schema().fields:
                # union_by_name() would also promote a type (e.g. int -> long) instead of raising
                if field.name in existing_types and field.field_type != existing_types[field.name]:
                    raise TypeError(f'column {field.name!r} is {field.field_type}, but {existing_types[field.name]} in the dataset')
            if mode == 'replace':
                tx.overwrite(table, overwrite_filter=partition_filter)
            else:
                tx.delete(And(partition_filter, EqualTo(self.IS_METADATA_COLUMN, True)))  # ty: ignore[missing-argument, too-many-positional-arguments]
                tx.append(table)

    def _read(
        self, key: DatasetKey, partitions: list[Partition] | None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        catalog = self._load_catalog()
        tbl = self._load_table(catalog, self._identifier(catalog, key))
        if tbl is None or (snapshot := tbl.current_snapshot()) is None:
            return None, {}
        # pinned to the current snapshot, so data and metadata match
        return self._read_marker_rows(key, pl.scan_iceberg(tbl, snapshot_id=snapshot.snapshot_id), partitions)

    def _tables(self) -> list[Table]:
        catalog = self._load_catalog()
        return [
            catalog.load_table(identifier)
            for (namespace, *_) in catalog.list_namespaces()
            for identifier in catalog.list_tables(namespace)
        ]

    def optimize(self) -> None:
        """See TableIO.optimize(). Rewrites each partition with more than one file into a single file."""
        for tbl in self._tables():
            self._with_retries(lambda tbl=tbl: self._compact(tbl.name()), f'optimize {tbl.name()}')

    def _compact(self, identifier: tuple[str, ...]) -> None:
        tbl = self._load_catalog().load_table(identifier)
        if (snapshot := tbl.current_snapshot()) is None:
            return
        partition_by = [field.name for field in tbl.spec().fields]
        file_counts: dict[Partition, int] = {}
        for partition in tbl.inspect.files(snapshot_id=snapshot.snapshot_id).column('partition').to_pylist():
            values = tuple(partition[col] for col in partition_by) if partition_by else ()
            file_counts[values] = file_counts.get(values, 0) + 1
        with tbl.transaction() as tx:
            for values, count in file_counts.items():
                if count == 1:
                    continue
                partition_filter = _partition_filter(partition_by, values)
                rows = tbl.scan(row_filter=partition_filter, snapshot_id=snapshot.snapshot_id).to_arrow()
                tx.overwrite(rows, overwrite_filter=partition_filter)

    def vacuum(
        self, *, retention: datetime.timedelta = TableIO.DEFAULT_VACUUM_RETENTION, dry_run: bool = True,
    ) -> VacuumResult:
        """See TableIO.vacuum().

        - expires snapshots that were replaced by a newer one more than `retention` ago
            (the current snapshot is never expired)
        - deletes files under the table's location that no remaining snapshot uses, once older than `retention`
            (so files of a write still committing aren't deleted)
        """
        self._check_retention(retention)
        cutoff = datetime.datetime.now(datetime.UTC) - retention
        result = VacuumResult()
        for tbl in self._tables():
            expired_ids, paths = self._with_retries(
                lambda tbl=tbl: self._vacuum_table(tbl.name(), cutoff, dry_run), f'vacuum {tbl.name()}',
            )
            name = '.'.join(tbl.name())
            result.expired_snapshots.extend(f'{name}@{snapshot_id}' for snapshot_id in expired_ids)
            result.deleted_paths.extend(paths)
        return result

    def _vacuum_table(
        self, identifier: tuple[str, ...], cutoff: datetime.datetime, dry_run: bool,
    ) -> tuple[list[int], list[str]]:
        """Returns (expired snapshot ids, deleted paths) of the table, or what would be expired/deleted if dry_run."""
        tbl = self._load_catalog().load_table(identifier)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        snapshots = sorted(tbl.snapshots(), key=lambda snapshot: snapshot.timestamp_ms)
        ref_ids = {ref.snapshot_id for ref in tbl.metadata.refs.values()}
        expired_ids = {
            snapshot.snapshot_id for snapshot, newer in itertools.pairwise(snapshots)
            if newer.timestamp_ms < cutoff_ms and snapshot.snapshot_id not in ref_ids
        }
        if expired_ids and not dry_run:
            tbl.maintenance.expire_snapshots().by_ids(list(expired_ids)).commit()
            tbl = self._load_catalog().load_table(identifier)

        used = {tbl.metadata_location, *(entry.metadata_file for entry in tbl.metadata.metadata_log)}
        used |= {statistics.statistics_path for statistics in tbl.metadata.statistics}
        for snapshot in tbl.snapshots():
            if snapshot.snapshot_id in expired_ids:
                continue  # only possible in a dry run
            used.add(snapshot.manifest_list)
            for manifest in snapshot.manifests(tbl.io):
                used.add(manifest.manifest_path)
                used |= {entry.data_file.file_path for entry in manifest.fetch_manifest_entry(tbl.io)}
        used_paths = {path.removeprefix('file://') for path in used}

        unused_paths = []
        for dir_path, _, file_names in os.walk(tbl.location().removeprefix('file://')):
            for file_name in file_names:
                path = os.path.join(dir_path, file_name)
                if path not in used_paths and os.path.getmtime(path) < cutoff.timestamp():
                    unused_paths.append(path)
        if not dry_run:
            for path in unused_paths:
                os.remove(path)
        return sorted(expired_ids), sorted(unused_paths)
