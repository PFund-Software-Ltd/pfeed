# pyright: reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnsafeMultipleInheritance=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from duckdb import DuckDBPyConnection

    from pfeed._io.base_io import MetadataDict
    from pfeed.data_handlers.base_data_handler import BaseDataMetadata

import json

import duckdb
import narwhals as nw
import polars as pl
import pyarrow.fs as pa_fs

from pfeed._io.database_io import DatabaseIO, DBPath
from pfeed._io.file_io import FileIO
from pfeed.enums import TimestampPrecision


class DuckDBIO(DatabaseIO, FileIO):
    # TODO: integrate with DuckDB's QUACK protocol to allow parallel writes
    SUPPORTS_PARALLEL_WRITES: bool = False
    FILE_EXTENSION = ".duckdb"
    TIMESTAMP_PRECISION: TimestampPrecision = TimestampPrecision.NANOSECOND
    DATE_FILTER_PREDICATE = (
        "{date_col} >= '{start_date}' AND {date_col} <= '{end_date}'"
    )

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
        FileIO.__init__(
            self,
            storage_options=storage_options,
            connect_options=connect_options,
            read_options=read_options,
            write_options=write_options,
            filesystem=filesystem,
        )
        # self._in_memory = self._storage_options['in_memory']
        # self._memory_limit = self._storage_options['memory_limit']

    def _open_connection(self, uri: str):
        self._conn: DuckDBPyConnection = duckdb.connect(uri, **self._connect_options)
        self._conn_uri = uri
        # if self._in_memory:
        #     if ";" in self._memory_limit or "'" in self._memory_limit:
        #         raise ValueError(f"Invalid memory_limit: {self._memory_limit!r}")
        #     _ = self._conn.execute(f"SET memory_limit = '{self._memory_limit}'")

    def _close_connection(self):
        if self._conn:
            self._conn.close()

    def exists(self, db_path: DBPath) -> bool:
        conn = self.connect(db_path.db_uri)
        schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
        schema_name, table_name = schema_qualified_table_name.split(".")
        return (
            conn.execute(
                """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
            AND table_name = ?
        """,
                (schema_name, table_name),
            ).fetchone()
            is not None
        )

    def is_empty(self, db_path: DBPath) -> bool:
        conn = self.connect(db_path.db_uri)
        schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
        try:
            # More efficient than COUNT(*) - just check if any row exists
            result = conn.execute(f"""
                SELECT 1 FROM {schema_qualified_table_name} LIMIT 1
            """).fetchone()
            return result is None
        except duckdb.CatalogException:
            # table does not exist -> treat as empty; all other errors (IO, permission, etc.) propagate
            return True

    def write(
        self,
        data: pd.DataFrame | pl.LazyFrame | pa.Table,
        db_path: DBPath,
        delete_where: str | None = None,
    ):
        """Write data to DuckDB table using delete-then-insert pattern.

        Args:
            data: Data to write (pandas/polars DataFrame or PyArrow table).
            db_path: Database path specifying schema and table.
            delete_where: SQL WHERE clause to delete existing rows before inserting.
                If None, data is appended (may cause duplicates if called twice).
                If provided, matching rows are deleted first, then new data is inserted.
                Example: "date >= '2024-01-01' AND date <= '2024-01-31'"

        Note:
            This is NOT a filter for SELECT - it controls which rows to DELETE
            before inserting, enabling idempotent writes for a given predicate.
        """
        conn = self.connect(db_path.db_uri)
        schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
        schema_name = schema_qualified_table_name.split(".")[0]

        # Safety net for direct callers: enforce IO-level schema policy. Callers
        # that need post-conform metadata (e.g. start/end ts for delete_where)
        # should call self.conform(data) explicitly before reaching here; doing
        # so makes this call a no-op since conform() is idempotent.
        data = self.conform(data)

        # Reject typed-null columns up front: without a real dtype, DuckDB picks
        # an arbitrary one (often INTEGER) for the table, and later writes silently
        # coerce real values (e.g. 100.5 -> 100). Affects polars pl.Null and
        # pyarrow pa.null() — narwhals surfaces both as nw.Unknown.
        null_cols = [
            name
            for name, dtype in nw.from_native(data).collect_schema().items()
            if dtype == nw.Unknown
        ]
        if null_cols and not self.exists(db_path):
            raise ValueError(
                f"Cannot create table {schema_qualified_table_name} from data with all-null columns: {null_cols}. "
                + "Cast these columns to a concrete dtype before writing."
            )

        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

            # Create a table with the same structure as df but without data by using WHERE 1=0 trick
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_qualified_table_name} AS
                SELECT * FROM data WHERE 1=0
            """)

            # BY NAME safely handles reordered columns, but DuckDB may fill a
            # missing target column with NULL/default. Require the exact same
            # column set so schema drift cannot be accepted silently.
            source_columns = list(nw.from_native(data).collect_schema().keys())
            target_columns = [
                row[0]
                for row in conn.execute(
                    f"DESCRIBE {schema_qualified_table_name}"
                ).fetchall()
            ]
            if set(source_columns) != set(target_columns):
                missing = sorted(set(target_columns) - set(source_columns))
                extra = sorted(set(source_columns) - set(target_columns))
                raise ValueError(
                    f"Schema mismatch for {schema_qualified_table_name}: "
                    + f"missing columns={missing}, extra columns={extra}"
                )

            # Delete any overlapping data before inserting
            if delete_where:
                conn.execute(f"""
                    DELETE FROM {schema_qualified_table_name}
                    WHERE {delete_where}
                """)

            # Match by name so a reordered input cannot silently swap values
            # between same-typed columns.
            conn.execute(
                f"INSERT INTO {schema_qualified_table_name} BY NAME SELECT * FROM data"
            )
            conn.execute("COMMIT")
        except Exception as exc:
            try:
                conn.execute("ROLLBACK")
            except Exception as rollback_exc:
                raise Exception(
                    f"Failed to rollback transaction after write failure ({db_path=}): {rollback_exc}"
                ) from exc
            raise Exception(
                f"Failed to write data (type={type(data)}) ({db_path=}); rolled back: {exc}"
            ) from exc

    def write_metadata(self, db_path: DBPath, metadata: BaseDataMetadata) -> None:
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            schema_name, table_name = schema_qualified_table_name.split(".")
            metadata_table = f"{schema_name}.{self.METADATA_TABLE_NAME}"
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            # 'updated_at' must be set explicitly on every write: DuckDB's
            # INSERT OR REPLACE behaves as in-place UPDATE on conflict, so a
            # column DEFAULT only fires on the very first insert.
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {metadata_table} (
                    table_name VARCHAR PRIMARY KEY,
                    metadata_json JSON,
                    updated_at TIMESTAMPTZ
                )
            """)
            metadata_json = json.dumps(metadata.model_dump(), default=str)
            conn.execute(
                f"""
                INSERT OR REPLACE INTO {metadata_table} (table_name, metadata_json, updated_at) VALUES (?, ?, now())
            """,
                (
                    table_name,
                    metadata_json,
                ),
            )
        except Exception as exc:
            raise Exception(
                f"Failed to write metadata (type={type(metadata)}) ({db_path=}): {exc}"
            ) from exc

    def read(self, db_path: DBPath, where: str | None = None) -> pl.LazyFrame | None:
        """Read a table from DuckDB, optionally filtered.

        Args:
            db_path: Database path specifying schema and table.
            where: SQL WHERE clause to filter rows on read. Same trust model as
                write()'s delete_where — callers construct this from internal,
                non-user-controlled values. Example: "date >= '2024-01-01'".
        """
        lf: pl.LazyFrame | None = None
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            if self.exists(db_path):
                sql = f"SELECT * FROM {schema_qualified_table_name}"
                if where:
                    sql += f" WHERE {where}"
                result = conn.execute(sql)
                # REVIEW: result.pl(lazy=True) returns a LazyFrame backed by the DuckDB result.
                # Any pushdown op (e.g. .head(1), .filter(...).collect()) triggers:
                #   INTERNAL Error: Attempted to dereference shared_ptr that is NULL!
                # Materializing eagerly then re-wrapping in .lazy() avoids the issue
                # at no extra cost (SELECT * is non-streaming anyway).
                lf = result.pl(lazy=False).lazy()
            return lf
        except Exception as exc:
            raise Exception(f"Failed to read data ({db_path=}): {exc}") from exc

    def read_metadata(self, db_path: DBPath) -> dict[DBPath, MetadataDict]:
        metadata: dict[DBPath, MetadataDict] = {}
        try:
            conn = self.connect(db_path.db_uri)
            schema_qualified_table_name = self._get_schema_qualified_table_name(db_path)
            schema_name, table_name = schema_qualified_table_name.split(".")
            metadata_table = f"{schema_name}.{self.METADATA_TABLE_NAME}"
            metadata_db_path = DBPath(
                db_uri=db_path.db_uri,
                db_name=db_path.db_name,
                schema_name=db_path.schema_name,
                table_name=self.METADATA_TABLE_NAME,
            )
            if self.exists(metadata_db_path):
                result = conn.execute(
                    f"""
                    SELECT metadata_json FROM {metadata_table}
                    WHERE table_name = ?
                """,
                    (table_name,),
                ).fetchone()
                if result is not None:
                    metadata[db_path] = json.loads(result[0])
        except Exception as exc:
            raise Exception(f"Failed to read metadata ({db_path=}): {exc}") from exc
        return metadata
