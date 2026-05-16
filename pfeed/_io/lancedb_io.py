# pyright: reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnsafeMultipleInheritance=false, reportAttributeAccessIssue=false, reportUnnecessaryComparison=false, reportUnknownArgumentType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow as pa
    from lancedb import LanceDBConnection
    from lancedb.pydantic import LanceModel
    from lancedb.table import LanceTable
    from pfeed._io.base_io import MetadataDict
    from pfeed.data_handlers.base_data_handler import BaseDataMetadata

import lancedb
import polars as pl
import pyarrow.fs as pa_fs

from pfeed.enums import TimestampPrecision
from pfeed._io.table_io import TableIO, TablePath
from pfeed._io.database_io import DatabaseIO, DBPath


class LanceDBIO(DatabaseIO, TableIO):
    # TODO: support streaming
    # SUPPORTS_STREAMING: bool = True
    SUPPORTS_PARALLEL_WRITES: bool = True
    METADATA_FILENAME: str = "lancedb_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage
    TIMESTAMP_PRECISION = TimestampPrecision.NANOSECOND
    DATE_FILTER_PREDICATE: str = "{date_col} >= cast('{start_date}' as timestamp) AND {date_col} <= cast('{end_date}' as timestamp)"

    def __init__(
        self,
        storage_options: dict[str, Any] | None = None,
        connect_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        write_options: dict[str, Any] | None = None,
        filesystem: pa_fs.FileSystem | None = None,
        **kwargs: Any,  # for compatibility with other IO classes
    ):
        DatabaseIO.__init__(
            self,
            storage_options=storage_options,
            connect_options=connect_options,
            read_options=read_options,
            write_options=write_options,
        )
        TableIO.__init__(
            self,
            storage_options=storage_options,
            connect_options=connect_options,
            read_options=read_options,
            write_options=write_options,
            filesystem=filesystem,
        )

    def _open_connection(self, uri: str):
        self._conn: LanceDBConnection = lancedb.connect(
            uri, storage_options=self._storage_options, **self._connect_options
        )
        self._conn_uri = uri

    def _close_connection(self):
        if self._conn is not None:
            lancedb_conn = self._conn._conn
            if lancedb_conn.is_open():
                _ = lancedb_conn.close()

    def exists(self, db_path: DBPath) -> bool:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return db_path.table_name in conn

    def is_empty(self, db_path: DBPath) -> bool:
        table = self.get_table(db_path)
        return table.count_rows() == 0

    def get_table(self, db_path: DBPath, **io_kwargs: Any) -> LanceTable:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return conn.open_table(db_path.table_name, storage_options=self._storage_options, **io_kwargs)

    def write(
        self,
        data: list[dict[str, Any]] | pa.Table,
        db_path: DBPath,
        delete_where: str | None = None,
        schema: pa.Schema | LanceModel | None = None,
        **io_kwargs: Any,
    ):
        """Write data to LanceDB table using delete-then-insert pattern.

        Args:
            data: Data to write as list of dicts or PyArrow table.
            db_path: Path to the LanceDB database.
            delete_where: SQL WHERE clause to delete existing rows before inserting.
                If None, data is appended (may cause duplicates if called twice).
                If provided, matching rows are deleted first, then new data is inserted,
                enabling idempotent writes for a given predicate.
                Example: "date >= '2024-01-15' AND date <= '2024-01-20'"
            schema: Optional schema for table creation.
        """
        io_kwargs = io_kwargs or self._write_options
        conn: LanceDBConnection = self.connect(db_path.db_uri)

        # Safety net for direct callers: enforce IO-level schema policy. Callers
        # that need post-conform metadata (e.g. start/end ts for delete_where)
        # should call self.conform(data) explicitly before reaching here; doing
        # so makes this call a no-op since conform() is idempotent.
        # Skip for list[dict] inputs — narwhals does not accept them and there
        # is no typed datetime precision to enforce at that stage.
        if not isinstance(data, list):
            data = self.conform(data)

        if not self.exists(db_path):
            table = conn.create_table(db_path.table_name, data, schema=schema, mode="overwrite", **io_kwargs)
        else:
            table = self.get_table(db_path)
            # LanceDB has no BEGIN/COMMIT, but tables are versioned: snapshot the
            # version before the delete+add, and on failure restore() rolls the
            # table back to that snapshot so the pair fails atomically (otherwise
            # a successful delete + failed add would leave a half-destroyed table).
            version_before = table.version
            try:
                if delete_where:
                    _ = table.delete(where=delete_where)
                _ = table.add(data)
            except Exception as exc:
                try:
                    table.restore(version_before)
                except Exception as restore_exc:
                    raise Exception(
                        f'Failed to restore table to version {version_before} after write failure ({db_path=}): {restore_exc}'
                    ) from exc
                raise Exception(
                    f'Failed to write data (type={type(data)}) ({db_path=}); rolled back to version {version_before}: {exc}'
                ) from exc

    def read(self, db_path: DBPath, where: str | None = None, **io_kwargs: Any) -> pl.LazyFrame | None:
        """Read a table from LanceDB, optionally filtered.

        Args:
            db_path: Path to the LanceDB database.
            where: SQL-style WHERE clause to filter rows on read. Same trust
                model as write()'s delete_where — callers construct this from
                internal, non-user-controlled values.
                Example: "date >= '2024-01-01'".
        """
        io_kwargs = io_kwargs or self._read_options
        lf: pl.LazyFrame | None = None
        if self.exists(db_path):
            table = self.get_table(db_path, **io_kwargs)
            # LanceDB returns LazyFrame from to_polars() but an eager DataFrame
            # from search().where().to_polars(); normalize to LazyFrame either way.
            result = table.search().where(where).to_polars() if where else table.to_polars()
            lf = result.lazy() if isinstance(result, pl.DataFrame) else result
        return lf

    def write_metadata(self, db_path: DBPath, metadata: BaseDataMetadata):
        return TableIO.write_metadata(
            self,
            table_path=TablePath(db_path.db_uri),
            metadata=metadata,
        )

    def read_metadata(
        self,
        db_path: DBPath,
        max_retries: int=5,
        base_delay: float=0.1,
    ) -> dict[DBPath, MetadataDict]:
        table_path = TablePath(db_path.db_uri)
        metadata: dict[TablePath, MetadataDict] = TableIO.read_metadata(
            self,
            table_path=table_path,
            max_retries=max_retries,
            base_delay=base_delay,
        )
        return { db_path: metadata[table_path] } if metadata else {}
