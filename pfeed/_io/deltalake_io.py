from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow as pa
    from pfeed.utils.file_path import FilePath
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from deltalake.table import FilterConjunctionType

import polars as pl
from deltalake import DeltaTable, write_deltalake

from pfeed._io.table_io import TableIO


class DeltaLakeIO(TableIO):
    SUPPORTS_STREAMING: bool = True
    SUPPORTS_PARTITIONING: bool = True
    METADATA_FILENAME: str = "deltalake_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage
    
    def exists(self, table_path: FilePath) -> bool:
        """Check if a Delta Lake table exists at this path."""
        return DeltaTable.is_deltatable(str(table_path), storage_options=self._storage_options)
    
    def is_empty(self, table_path: FilePath, partition_filters: FilterConjunctionType | None = None) -> bool:
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
    
    def get_table(self, table_path: FilePath, **io_kwargs) -> DeltaTable:
        return DeltaTable(str(table_path), storage_options=self._storage_options, **io_kwargs)

    def write(
        self,
        data: pa.Table,
        table_path: FilePath,
        where: str | None = None,
        partition_by: list[str] | None=None,
        max_retries: int=5,
        base_delay: float=0.1,
        **io_kwargs,
    ):
        """Write data to Delta Lake with retry logic for concurrent write conflicts.

        Delta Lake's transaction log can fail with "version X already exists" 
        when multiple writers try to create the same version
        simultaneously (especially common for version 0).

        Args:
            data: PyArrow table to write.
            table_path: Path to the Delta Lake table.
            where: Optional filter to replace only matching rows (triggers overwrite mode).
                If None, data is appended (creates table if needed). If provided, only rows
                matching the clause are replaced (e.g., "date = '2024-01-15'" or
                "year = 2024 AND month = 1").
            partition_by: List of columns to partition the table by. Only required
                when creating a new table.
            max_retries: Maximum number of retry attempts for concurrent write conflicts.
            base_delay: Initial delay in seconds, doubles with each retry (exponential backoff).
        """
        import time
        import random

        last_exception = None
        for attempt in range(max_retries):
            try:
                write_deltalake(
                    str(table_path),
                    data,
                    mode='overwrite' if where else 'append',
                    storage_options=self._storage_options,
                    partition_by=partition_by,
                    predicate=where,
                    **io_kwargs,
                )
                break
            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()
                # Retry on transaction conflicts (e.g., "version 0 already exists")
                if 'already exists' in error_msg or 'transaction failed' in error_msg:
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        delay = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
                        time.sleep(delay)
                        continue
                raise
        else:
            # All retries exhausted
            raise last_exception

    def read(self, table_path: FilePath, **io_kwargs) -> pl.LazyFrame | None:
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
        lf: pl.LazyFrame | None = None
        if self.exists(table_path):
            dt = self.get_table(table_path, **io_kwargs)
            lf = pl.scan_delta(dt, use_pyarrow=True)
        return lf
