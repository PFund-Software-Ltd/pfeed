# pyright: reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnsafeMultipleInheritance=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from sqlite3 import Connection as SQLiteConnection

    import pandas as pd

    from pfeed._io.base_io import MetadataDict
    from pfeed.data_handlers.base_data_handler import BaseDataMetadata

import datetime
import json
import math
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import narwhals as nw
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

from pfeed._io.database_io import DatabaseIO, DBPath
from pfeed._io.file_io import FileIO
from pfeed.enums import TimestampPrecision


class SQLiteIO(DatabaseIO, FileIO):
    SUPPORTS_PARALLEL_WRITES: bool = False
    FILE_EXTENSION = ".sqlite"
    TIMESTAMP_PRECISION: TimestampPrecision = TimestampPrecision.MICROSECOND
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
        **kwargs: Any,
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

    @staticmethod
    def _quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _physical_table_name(self, db_path: DBPath) -> str:
        # SQLite has no user-defined schemas. Quoting the full dotted name makes
        # `schema.table` one physical identifier instead of an attached DB name.
        return self._get_schema_qualified_table_name(db_path)

    def _quoted_table_name(self, db_path: DBPath) -> str:
        return self._quote_identifier(self._physical_table_name(db_path))

    def _metadata_table_name(self, db_path: DBPath) -> str:
        if db_path.schema_name is None:
            raise ValueError("schema_name must be provided")
        schema_name = self._sanitize_identifier(db_path.schema_name)
        return f"{schema_name}.{self.METADATA_TABLE_NAME}"

    def _quoted_metadata_table_name(self, db_path: DBPath) -> str:
        return self._quote_identifier(self._metadata_table_name(db_path))

    def _open_connection(self, uri: str):
        if "://" in uri and not uri.startswith("file:"):
            raise NotImplementedError(
                "SQLiteIO only supports local database files and SQLite file: URIs"
            )
        if uri != ":memory:" and not uri.startswith("file:"):
            Path(uri).parent.mkdir(parents=True, exist_ok=True)
        self._conn: SQLiteConnection = sqlite3.connect(uri, **self._connect_options)
        self._conn_uri = uri

    def _close_connection(self):
        if self._conn is not None:
            self._conn.close()

    @contextmanager
    def _savepoint(self, conn: SQLiteConnection, name: str):
        conn.execute(f"SAVEPOINT {name}")
        try:
            yield
        except Exception:
            conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
            conn.execute(f"RELEASE SAVEPOINT {name}")
            raise
        else:
            conn.execute(f"RELEASE SAVEPOINT {name}")

    @staticmethod
    def _table_exists(conn: SQLiteConnection, physical_name: str) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (physical_name,),
            ).fetchone()
            is not None
        )

    def exists(self, db_path: DBPath) -> bool:
        conn: SQLiteConnection = self.connect(db_path.db_uri)
        return self._table_exists(conn, self._physical_table_name(db_path))

    def is_empty(self, db_path: DBPath) -> bool:
        conn: SQLiteConnection = self.connect(db_path.db_uri)
        if not self._table_exists(conn, self._physical_table_name(db_path)):
            return True
        result = conn.execute(
            f"SELECT 1 FROM {self._quoted_table_name(db_path)} LIMIT 1"
        ).fetchone()
        return result is None

    @staticmethod
    def _normalize_schema(schema: pa.Schema) -> pa.Schema:
        return pa.schema(
            [
                pa.field(field.name, field.type, nullable=field.nullable)
                for field in schema
            ]
        )

    def _to_arrow(
        self, data: pd.DataFrame | pl.DataFrame | pl.LazyFrame | pa.Table
    ) -> pa.Table:
        conformed = self.conform(data)
        arrow_table: pa.Table
        if isinstance(conformed, pa.Table):
            # Avoid a needless Arrow -> Narwhals -> Arrow round trip, which can
            # change native Arrow types (for example date64 to timestamp[ms]).
            arrow_table = conformed
        else:
            frame = nw.from_native(conformed)
            if isinstance(frame, nw.LazyFrame):
                frame = frame.collect()
            arrow_table = cast(pa.Table, frame.to_arrow())
        schema = self._normalize_schema(arrow_table.schema)
        return pa.Table.from_arrays(arrow_table.columns, schema=schema)

    @staticmethod
    def _is_supported_type(dtype: pa.DataType) -> bool:
        return any(
            (
                pa.types.is_null(dtype),
                pa.types.is_boolean(dtype),
                pa.types.is_integer(dtype),
                pa.types.is_floating(dtype),
                # Polars cannot import Arrow decimal256 safely; SQLiteIO.read()
                # returns Polars, so accept only the interoperable decimal128.
                pa.types.is_decimal128(dtype),
                pa.types.is_string(dtype),
                pa.types.is_large_string(dtype),
                pa.types.is_binary(dtype),
                pa.types.is_large_binary(dtype),
                pa.types.is_fixed_size_binary(dtype),
                pa.types.is_date(dtype),
                pa.types.is_time32(dtype),
                pa.types.is_time64(dtype),
                pa.types.is_timestamp(dtype),
                pa.types.is_duration(dtype),
            )
        )

    @staticmethod
    def _sqlite_affinity(dtype: pa.DataType) -> str:
        if pa.types.is_boolean(dtype) or pa.types.is_integer(dtype):
            return "INTEGER"
        if pa.types.is_floating(dtype):
            return "REAL"
        if (
            pa.types.is_binary(dtype)
            or pa.types.is_large_binary(dtype)
            or pa.types.is_fixed_size_binary(dtype)
        ):
            return "BLOB"
        if (
            pa.types.is_time32(dtype)
            or pa.types.is_time64(dtype)
            or pa.types.is_duration(dtype)
        ):
            return "INTEGER"
        return "TEXT"

    def _ensure_metadata_table(self, conn: SQLiteConnection, db_path: DBPath) -> None:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._quoted_metadata_table_name(db_path)} (
                table_name TEXT PRIMARY KEY,
                metadata_json TEXT,
                schema_ipc BLOB,
                updated_at TEXT NOT NULL
            )
        """)

    def _read_stored_schema(
        self, conn: SQLiteConnection, db_path: DBPath
    ) -> pa.Schema | None:
        if not self._table_exists(conn, self._metadata_table_name(db_path)):
            return None
        table_name = self._sanitize_identifier(db_path.table_name)
        result = conn.execute(
            f"SELECT schema_ipc FROM {self._quoted_metadata_table_name(db_path)} "
            + "WHERE table_name = ?",
            (table_name,),
        ).fetchone()
        if result is None or result[0] is None:
            return None
        return self._normalize_schema(pa.ipc.read_schema(pa.BufferReader(result[0])))

    def _infer_schema(self, conn: SQLiteConnection, physical_name: str) -> pa.Schema:
        fields: list[pa.Field] = []
        rows = conn.execute(
            'SELECT name, type, "notnull" FROM pragma_table_info(?)',
            (physical_name,),
        ).fetchall()
        for name, declared_type, not_null in rows:
            sqlite_type = (declared_type or "").upper()
            if "INT" in sqlite_type:
                dtype = pa.int64()
            elif any(token in sqlite_type for token in ("REAL", "FLOA", "DOUB")):
                dtype = pa.float64()
            elif "BLOB" in sqlite_type or not sqlite_type:
                dtype = pa.binary()
            else:
                dtype = pa.string()
            fields.append(pa.field(name, dtype, nullable=not bool(not_null)))
        return pa.schema(fields)

    @staticmethod
    def _align_table(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
        source_names = set(table.column_names)
        target_names = set(target_schema.names)
        if source_names != target_names:
            missing = sorted(target_names - source_names)
            extra = sorted(source_names - target_names)
            raise ValueError(
                f"Schema mismatch: missing columns={missing}, extra columns={extra}"
            )

        arrays: list[pa.ChunkedArray] = []
        for field in target_schema:
            column = table[field.name]
            if pa.types.is_null(column.type):
                column = pa.chunked_array(
                    [pa.nulls(len(table), type=field.type)], type=field.type
                )
            elif column.type != field.type:
                column = pc.cast(column, field.type, safe=True)
            arrays.append(column)
        return pa.Table.from_arrays(arrays, schema=target_schema)

    @staticmethod
    def _encode_column(column: pa.ChunkedArray, dtype: pa.DataType) -> list[Any]:
        # Preserve the physical unit count for Arrow time/duration values. Going
        # through Python's datetime.time would truncate time64[ns] to microseconds.
        if pa.types.is_time32(dtype):
            return pc.cast(pc.cast(column, pa.int32()), pa.int64()).to_pylist()
        if pa.types.is_time64(dtype) or pa.types.is_duration(dtype):
            return pc.cast(column, pa.int64()).to_pylist()

        encoded: list[Any] = []
        for value in column.to_pylist():
            if value is None:
                encoded.append(None)
            elif pa.types.is_boolean(dtype):
                encoded.append(int(value))
            elif pa.types.is_integer(dtype):
                integer = int(value)
                if not -(2**63) <= integer <= 2**63 - 1:
                    raise OverflowError(
                        f"Integer value {integer} is outside SQLite's signed 64-bit range"
                    )
                encoded.append(integer)
            elif pa.types.is_floating(dtype):
                number = float(value)
                if math.isnan(number):
                    raise ValueError(
                        "SQLite cannot round-trip NaN without converting it to NULL"
                    )
                encoded.append(number)
            elif pa.types.is_decimal(dtype):
                encoded.append(str(value))
            elif pa.types.is_timestamp(dtype):
                timestamp = value
                # Match Python/Polars' string form used by DATE_FILTER_PREDICATE:
                # whole-second values omit the fractional part, while non-zero
                # microseconds retain it. Keep timezone-aware values in the Arrow
                # schema's timezone too, because the predicate boundaries produced
                # by pfeed use that timezone; normalizing only the stored value to
                # UTC would make an identical delete-then-insert write duplicate it.
                encoded.append(timestamp.isoformat(sep=" "))
            elif pa.types.is_date(dtype):
                encoded.append(value.isoformat())
            elif (
                pa.types.is_binary(dtype)
                or pa.types.is_large_binary(dtype)
                or pa.types.is_fixed_size_binary(dtype)
            ):
                encoded.append(bytes(value))
            else:
                encoded.append(str(value))
        return encoded

    @staticmethod
    def _decode_value(value: Any, dtype: pa.DataType) -> Any:
        if value is None:
            return None
        if pa.types.is_boolean(dtype):
            return bool(value)
        if (
            pa.types.is_integer(dtype)
            or pa.types.is_time32(dtype)
            or pa.types.is_time64(dtype)
            or pa.types.is_duration(dtype)
        ):
            return int(value)
        if pa.types.is_floating(dtype):
            return float(value)
        if pa.types.is_decimal(dtype):
            return Decimal(value)
        if pa.types.is_timestamp(dtype):
            timestamp = datetime.datetime.fromisoformat(value)
            if timestamp.tzinfo is None:
                epoch = datetime.datetime(1970, 1, 1)
            else:
                timestamp = timestamp.astimezone(datetime.UTC)
                epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
            delta = timestamp - epoch
            total_nanoseconds = (
                (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
            ) * 1_000
            divisor = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[
                dtype.unit
            ]
            # Passing the physical epoch count avoids PyArrow reinterpreting an
            # aware Python datetime when constructing a timestamp-with-timezone.
            return total_nanoseconds // divisor
        if pa.types.is_date(dtype):
            return datetime.date.fromisoformat(value)
        if (
            pa.types.is_binary(dtype)
            or pa.types.is_large_binary(dtype)
            or pa.types.is_fixed_size_binary(dtype)
        ):
            return bytes(value)
        return value

    def write(
        self,
        data: pd.DataFrame | pl.DataFrame | pl.LazyFrame | pa.Table,
        db_path: DBPath,
        delete_where: str | None = None,
    ):
        """Atomically delete matching rows and insert data into SQLite."""
        conn: SQLiteConnection = self.connect(db_path.db_uri)
        table = self._to_arrow(data)
        display_name = self._physical_table_name(db_path)
        quoted_table = self._quoted_table_name(db_path)

        if not table.column_names:
            raise ValueError(
                f"Cannot create or write a zero-column table: {display_name}"
            )
        unsupported = [
            field.name
            for field in table.schema
            if not self._is_supported_type(field.type)
        ]
        if unsupported:
            raise TypeError(
                f"SQLiteIO does not support these column types: {unsupported}"
            )

        table_exists = self._table_exists(conn, display_name)
        null_cols = [
            field.name for field in table.schema if pa.types.is_null(field.type)
        ]
        if null_cols and not table_exists:
            raise ValueError(
                f"Cannot create table {display_name} from data with all-null columns: {null_cols}. "
                + "Cast these columns to a concrete dtype before writing."
            )

        try:
            with self._savepoint(conn, "pfeed_write"):
                if table_exists:
                    target_schema = self._read_stored_schema(conn, db_path)
                    if target_schema is None:
                        target_schema = self._infer_schema(conn, display_name)
                    table = self._align_table(table, target_schema)
                else:
                    target_schema = table.schema
                    column_defs = ", ".join(
                        f"{self._quote_identifier(field.name)} {self._sqlite_affinity(field.type)}"
                        for field in target_schema
                    )
                    conn.execute(f"CREATE TABLE {quoted_table} ({column_defs})")

                self._ensure_metadata_table(conn, db_path)
                table_name = self._sanitize_identifier(db_path.table_name)
                conn.execute(
                    f"""
                    INSERT INTO {self._quoted_metadata_table_name(db_path)}
                        (table_name, schema_ipc, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(table_name) DO UPDATE SET
                        schema_ipc = excluded.schema_ipc,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (table_name, target_schema.serialize().to_pybytes()),
                )

                if delete_where:
                    conn.execute(f"DELETE FROM {quoted_table} WHERE {delete_where}")

                encoded_columns = [
                    self._encode_column(table[field.name], field.type)
                    for field in target_schema
                ]
                placeholders = ", ".join("?" for _ in target_schema)
                columns = ", ".join(
                    self._quote_identifier(field.name) for field in target_schema
                )
                conn.executemany(
                    f"INSERT INTO {quoted_table} ({columns}) VALUES ({placeholders})",
                    zip(*encoded_columns, strict=True),
                )
        except Exception as exc:
            raise Exception(
                f"Failed to write data (type={type(data)}) ({db_path=}): {exc}"
            ) from exc

    def read(self, db_path: DBPath, where: str | None = None) -> pl.LazyFrame | None:
        try:
            conn: SQLiteConnection = self.connect(db_path.db_uri)
            physical_name = self._physical_table_name(db_path)
            if not self._table_exists(conn, physical_name):
                return None

            sql = f"SELECT * FROM {self._quoted_table_name(db_path)}"
            if where:
                sql += f" WHERE {where}"
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            schema = self._read_stored_schema(conn, db_path)
            if schema is None:
                schema = self._infer_schema(conn, physical_name)
            if column_names != schema.names:
                raise ValueError(
                    f"Stored schema columns {schema.names} do not match query columns {column_names}"
                )

            arrays: list[pa.Array] = []
            for index, field in enumerate(schema):
                values = [self._decode_value(row[index], field.type) for row in rows]
                arrays.append(pa.array(values, type=field.type))
            frame = cast(
                pl.DataFrame,
                pl.from_arrow(pa.Table.from_arrays(arrays, schema=schema)),
            )
            return frame.lazy()
        except Exception as exc:
            raise Exception(f"Failed to read data ({db_path=}): {exc}") from exc

    def write_metadata(self, db_path: DBPath, metadata: BaseDataMetadata) -> None:
        try:
            conn: SQLiteConnection = self.connect(db_path.db_uri)
            with self._savepoint(conn, "pfeed_metadata"):
                self._ensure_metadata_table(conn, db_path)
                table_name = self._sanitize_identifier(db_path.table_name)
                metadata_json = json.dumps(
                    metadata.model_dump(mode="json", fallback=str)
                )
                conn.execute(
                    f"""
                    INSERT INTO {self._quoted_metadata_table_name(db_path)}
                        (table_name, metadata_json, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(table_name) DO UPDATE SET
                        metadata_json = excluded.metadata_json,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (table_name, metadata_json),
                )
        except Exception as exc:
            raise Exception(
                f"Failed to write metadata (type={type(metadata)}) ({db_path=}): {exc}"
            ) from exc

    def read_metadata(self, db_path: DBPath) -> dict[DBPath, MetadataDict]:
        metadata: dict[DBPath, MetadataDict] = {}
        try:
            conn: SQLiteConnection = self.connect(db_path.db_uri)
            if not self._table_exists(conn, self._metadata_table_name(db_path)):
                return metadata
            table_name = self._sanitize_identifier(db_path.table_name)
            result = conn.execute(
                f"SELECT metadata_json FROM {self._quoted_metadata_table_name(db_path)} "
                + "WHERE table_name = ?",
                (table_name,),
            ).fetchone()
            if result is not None and result[0] is not None:
                metadata[db_path] = json.loads(result[0])
            return metadata
        except Exception as exc:
            raise Exception(f"Failed to read metadata ({db_path=}): {exc}") from exc

    def list_tables(self, db_path: DBPath, include_schema: bool = True) -> list[str]:
        conn: SQLiteConnection = self.connect(db_path.db_uri)
        if db_path.schema_name is None:
            raise ValueError("schema_name must be provided")
        schema_name = self._sanitize_identifier(db_path.schema_name)
        prefix = f"{schema_name}."
        metadata_name = f"{prefix}{self.METADATA_TABLE_NAME}"
        physical_names = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            ).fetchall()
        ]
        logical_names = [
            name[len(prefix) :]
            for name in physical_names
            if name.startswith(prefix) and name != metadata_name
        ]
        if include_schema:
            return [f"{schema_name}.{name}" for name in logical_names]
        return logical_names
