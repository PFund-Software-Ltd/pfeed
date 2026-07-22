# pyright: reportUnknownVariableType=false,reportUnknownMemberType=false, reportMissingSuperCall=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Self, TypeAlias

from typing_extensions import override

if TYPE_CHECKING:
    import pyarrow as pa
    from narwhals.typing import IntoFrame

    DBConnection: TypeAlias = Any  # e.g. DuckDBPyConnection | LanceDBConnection | PostgresConnection | AsyncPostgresConnection
    SchemaQualifiedTableName: TypeAlias = str

import warnings
from abc import ABC, abstractmethod

import narwhals as nw

from pfeed._io.base_io import BaseIO
from pfeed.enums import TimestampPrecision

_NARWHALS_TIMEUNIT: dict[TimestampPrecision, Literal["ms", "us", "ns"]] = {
    TimestampPrecision.MILLISECOND: "ms",
    TimestampPrecision.MICROSECOND: "us",
    TimestampPrecision.NANOSECOND: "ns",
}


# this is not an actual path, but carrier of information to where the db is, including db name, schema name and table name etc.
# calling it a path is just for consistency with FilePath and TablePath
class DBPath(NamedTuple):
    # e.g. for postgresql, it is "postgresql://user:password@localhost:5432/{db_name}"
    # for duckdb, it is the data path to the .duckdb file, e.g. "/path/to/data/{db_name}.duckdb"
    db_uri: str
    db_name: str
    table_name: str
    schema_name: str | None = None


# NOTE: only support SQL databases
class DatabaseIO(BaseIO, ABC):
    TIMESTAMP_PRECISION: TimestampPrecision = TimestampPrecision.MICROSECOND
    METADATA_TABLE_NAME: str = "metadata"

    def __init__(
        self,
        storage_options: dict[str, Any] | None = None,
        connect_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        write_options: dict[str, Any] | None = None,
        **kwargs: Any,  # for compatibility with other IO classes
    ):
        super().__init__(
            storage_options=storage_options,
            connect_options=connect_options,
            read_options=read_options,
            write_options=write_options,
        )
        self._conn: DBConnection | None = None
        self._conn_uri: str | None = None

    def conform(self, data: IntoFrame | pa.Table) -> IntoFrame:
        """Apply IO-level schema policy. Currently: cast Datetime columns to TIMESTAMP_PRECISION.

        Accepts any narwhals-supported frame (polars LazyFrame/DataFrame, pandas DataFrame,
        pyarrow Table) and returns the same native type. Idempotent — calling repeatedly is a
        no-op once the data already matches. Emits a warning when a cast is performed so
        callers can audit precision loss.
        """
        if self.TIMESTAMP_PRECISION not in _NARWHALS_TIMEUNIT:
            raise NotImplementedError(
                f"conform() does not support TIMESTAMP_PRECISION={self.TIMESTAMP_PRECISION} "
                + "(narwhals Datetime supports only ms/us/ns)"
            )
        target_unit = _NARWHALS_TIMEUNIT[self.TIMESTAMP_PRECISION]
        frame = nw.from_native(data)
        schema = frame.collect_schema()
        cast_cols: list[tuple[str, str]] = []
        casts: list[Any] = []
        for col, dtype in schema.items():
            if isinstance(dtype, nw.Datetime) and dtype.time_unit != target_unit:
                cast_cols.append((col, dtype.time_unit))
                casts.append(
                    nw.col(col).cast(nw.Datetime(target_unit, dtype.time_zone))
                )
        if not casts:
            return data
        warnings.warn(
            f"Casting datetime columns to {self.TIMESTAMP_PRECISION} for "
            + f"{type(self).__name__}: "
            + ", ".join(f"{c} ({u} -> {target_unit})" for c, u in cast_cols),
            RuntimeWarning,
            stacklevel=2,
        )
        return frame.with_columns(*casts).to_native()

    def _get_schema_qualified_table_name(
        self, db_path: DBPath
    ) -> SchemaQualifiedTableName:
        table_name = self._sanitize_identifier(db_path.table_name)
        if db_path.schema_name is None:
            raise ValueError("schema_name must be provided")
        schema_name = self._sanitize_identifier(db_path.schema_name)
        return f"{schema_name}.{table_name}"

    # REVIEW:
    def _sanitize_identifier(self, name: str) -> str:
        return (
            name.replace("-", "_")
            .replace(":", "_")
            .replace(".", "p")  # 123456.123 -> 123456p123, e.g. for strike price
            .replace("/", "_")  # for crypto pairs like BTC/USDT
            .replace(" ", "_")  # spaces
            .replace(";", "_")  # prevent statement termination
            .replace("'", "_")  # prevent string breakout
            .replace('"', "_")  # prevent identifier breakout
            .replace("\\", "_")  # prevent escape sequences
            .replace("\x00", "")  # null byte
        )

    def connect(self, uri: str) -> DBConnection:
        if self._conn is None:
            self._open_connection(uri)
        else:
            # if its opening a different file
            if uri != self._conn_uri:
                # Reset state before opening the replacement. If opening the new
                # URI fails, do not leave a closed connection cached as usable.
                self.disconnect()
                self._open_connection(uri)
        return self._conn

    def disconnect(self):
        """Explicitly close the database connection."""
        if self._conn is not None:
            self._close_connection()
            self._conn = None
            self._conn_uri = None

    @abstractmethod
    def write(
        self,
        data: Any,
        db_path: DBPath,
        delete_where: str | None = None,
        **io_kwargs: Any,
    ):
        pass

    @abstractmethod
    def read(self, db_path: DBPath, where: str | None = None, **io_kwargs: Any) -> Any:
        pass

    @abstractmethod
    def exists(self, db_path: DBPath) -> bool:
        pass

    @abstractmethod
    def is_empty(self, db_path: DBPath) -> bool:
        pass

    @abstractmethod
    def _open_connection(self, uri: str):
        pass

    @abstractmethod
    def _close_connection(self):
        pass

    @override
    def __enter__(self) -> Self:
        return self  # Setup - returns the object to be used in 'with'

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.disconnect()  # Cleanup - always runs at end of 'with' block
