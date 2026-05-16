# pyright: reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow as pa
    from deltalake.table import FilterConjunctionType

import time
import random

import polars as pl
from deltalake import DeltaTable, write_deltalake

from pfeed._io.table_io import TableIO, TablePath


class DeltaLakeIO(TableIO):
    SUPPORTS_STREAMING: bool = True
    SUPPORTS_PARALLEL_WRITES: bool = True
    SUPPORTS_PARTITIONING: bool = True
    METADATA_FILENAME: str = "deltalake_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage
    DATE_FILTER_PREDICATE: str = "{date_col} >= '{start_ts}' AND {date_col} <= '{end_ts}'"

    def exists(self, table_path: TablePath) -> bool:
        """Check if a Delta Lake table exists at this path."""
        if not super().exists(table_path):
            return False
        # NOTE: this DeltaTable.is_deltatable will somehow automatically create the directory if it doesn't exist
        # so we need to check if the directory exists to avoid creating a new dir as much as possible
        return DeltaTable.is_deltatable(str(table_path), storage_options=self._storage_options)

    def is_empty(self, table_path: TablePath, partition_filters: FilterConjunctionType | None = None) -> bool:
        """Check if a Delta Lake table or partition is empty.

        Args:
            table_path: Path to the Delta Lake table.
            partition_filters: Optional partition filters to check a specific partition.
                Example: [("year", "=", 2024), ("month", "=", 1), ("day", "=", 15)]

        Returns:
            True if the table/partition has no data files, False otherwise.

        Use cases:
            1. Check if entire table is empty: is_empty(table_path)
            2. Check if specific partition is empty: is_empty(table_path, partition_filters=[...])
            This is useful for checking if data exists for a specific date.
        """
        dt = self.get_table(table_path)
        return len(dt.file_uris(partition_filters=partition_filters)) == 0

    def get_table(self, table_path: TablePath, **io_kwargs: Any) -> DeltaTable:
        return DeltaTable(str(table_path), storage_options=self._storage_options, **io_kwargs)

    def write(
        self,
        data: pa.Table,
        table_path: TablePath,
        delete_where: str | None = None,
        partition_by: list[str] | None = None,
        max_retries: int = 5,
        base_delay: float = 0.1,
        **io_kwargs: Any,
    ) -> None:
        """Write data to Delta Lake with retry on concurrent transaction conflicts.

        Delta Lake's transaction log can fail with "version X already exists" when
        multiple writers race to commit the same version (especially version 0).

        Args:
            data: PyArrow table to write.
            table_path: Path to the Delta Lake table.
            delete_where: Optional filter to replace only matching rows (triggers overwrite mode).
                If None, data is appended (creates table if needed). If provided, only rows
                matching the clause are replaced (e.g., "date = '2024-01-15'").
            partition_by: Columns to partition the table by. Only required when creating
                a new table.
            max_retries: Maximum number of retry attempts for transaction conflicts.
            base_delay: Initial delay in seconds; doubles each retry (exponential backoff).

        Raises:
            Exception: The most recent error from `write_deltalake` if the write fails
                with a non-retriable error, or if all retries are exhausted.
        """
        io_kwargs = io_kwargs or self._write_options
        for attempt in range(max_retries):
            try:
                write_deltalake(
                    str(table_path),
                    data,
                    mode='overwrite' if delete_where else 'append',  # pyright: ignore[reportArgumentType]
                    storage_options=self._storage_options,
                    partition_by=partition_by,
                    predicate=delete_where,
                    **io_kwargs,
                )
                return
            except Exception as e:
                error_msg = str(e).lower()
                retriable = 'already exists' in error_msg or 'transaction failed' in error_msg
                if not retriable or attempt == max_retries - 1:
                    raise
                delay = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
                time.sleep(delay)

    def read(self, table_path: TablePath, **io_kwargs: Any) -> pl.LazyFrame | None:
        """Read data from a Delta Lake table.

        Args:
            table_path: Path to the Delta Lake table.
            **io_kwargs: Delta table options passed to DeltaTable constructor.
                Common options:
                    version (int | str | datetime | None): Read a specific table version.
                        If None, reads the latest version.

                See delta-rs DeltaTable documentation for all available options:
                https://delta-io.github.io/delta-rs/python/api_reference.html

        Returns:
            LazyFrame with table data, or None if table doesn't exist.
        """
        io_kwargs = io_kwargs or self._read_options
        lf: pl.LazyFrame | None = None
        if self.exists(table_path):
            dt = self.get_table(table_path, **io_kwargs)
            # NOTE: use_pyarrow=False doesn't work well with narwhals, e.g. narwhals.exceptions.NarwhalsError: path contains column not present in the given Hive schema: "env"
            lf = pl.scan_delta(dt, use_pyarrow=True)
        return lf
