from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.enums import Compression
    import pyarrow as pa
    from pfeed.utils.file_path import FilePath
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from deltalake.table import FilterConjunctionType

import polars as pl
from deltalake import DeltaTable, write_deltalake

from pfeed._io.base_io import BaseIO


class DeltaLakeIO(BaseIO):
    IS_TABLE_FORMAT: bool = True
    SUPPORTS_STREAMING: bool = True
    METADATA_FILENAME: str = "deltalake_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage
    
    def __init__(
        self,
        filesystem: pa_fs.FileSystem,
        storage_options: dict,
        compression: Compression | str | None=None, 
    ):
        super().__init__(filesystem=filesystem, storage_options=storage_options, compression=None)
    
    def exists(self, file_path: FilePath) -> bool:
        return DeltaTable.is_deltatable(str(file_path), storage_options=self._storage_options)
    
    def is_empty(self, file_path: FilePath, partition_filters: FilterConjunctionType | None = None) -> bool:
        """Check if a Delta Lake table or partition is empty.
    
        Args:
            file_path: Path to the Delta Lake table.
            partition_filters: Optional partition filters to check a specific partition.
                Example: [("year", "=", 2024), ("month", "=", 1), ("day", "=", 15)]
        
        Returns:
            True if the table/partition has no data files, False otherwise.
        
        Use cases:
            1. Check if entire table is empty: is_empty(table_path)
            2. Check if specific partition is empty: is_empty(table_path, partition_filters=[...])
            This is useful for checking if data exists for a specific date.
        """
        dt = self._read_delta_table(file_path)
        return len(dt.file_uris(partition_filters=partition_filters)) == 0
    
    def _read_delta_table(self, file_path: FilePath, **io_kwargs) -> DeltaTable:
        return DeltaTable(str(file_path), storage_options=self._storage_options, **io_kwargs)

    def write(
        self, 
        data: pa.Table, 
        file_path: FilePath, 
        metadata: BaseMetadataModel | None=None, 
        predicate: str | None = ...,
        partition_by: list[str] | None=None,
        max_retries: int=5,
        base_delay: float=0.1,
    ):
        """Write data to Delta Lake with retry logic for concurrent write conflicts.

        Delta Lake's transaction log can fail with "version X already exists" when
        multiple writers try to create the same version simultaneously. This is
        especially common for version 0 (initial table creation).

        Args:
            file_path: Path to the Delta Lake table.
            data: Data to write.
            partition_by: List of columns to partition the table by. Only required
                when creating a new table.
            predicate: When using `Overwrite` mode, replace data that matches a predicate.'
            max_retries: Maximum number of retry attempts.
            base_delay: Initial delay in seconds, doubles with each retry (exponential backoff).
        """
        import time
        import random

        last_exception = None
        for attempt in range(max_retries):
            try:
                write_deltalake(
                    str(file_path),
                    data,
                    mode='overwrite' if predicate else 'append',
                    storage_options=self._storage_options,
                    partition_by=partition_by,
                    predicate=predicate,
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

        if metadata:
            import pandas as pd
            # HACK: delta-rs doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
            empty_df_with_metadata = pd.DataFrame()
            table = pa.Table.from_pandas(empty_df_with_metadata, preserve_index=False)
            metadata_file_path = file_path / self.METADATA_FILENAME
            self._write_pyarrow_table_with_metadata(table, metadata_file_path, metadata=metadata)
    
    def read(self, file_paths: list[FilePath], **io_kwargs) -> pl.LazyFrame | None:
        """Read data from a Delta Lake table.

        Args:
            file_paths: List containing single path to Delta Lake table.
            **io_kwargs: Delta table options passed to DeltaTable constructor.
                Common options:
                    version (int | str | datetime | None): Read a specific table version.
                        If None, reads the latest version.

                See delta-rs DeltaTable documentation for all available options:
                https://delta-io.github.io/delta-rs/python/api_reference.html

        Returns:
            LazyFrame with table data, or None if table doesn't exist.
        """
        assert len(file_paths) == 1, 'deltalake table should have exactly one file path'
        table_path = file_paths[0]
        lf: pl.LazyFrame | None = None
        if self.exists(table_path):
            dt = self._read_delta_table(table_path, **io_kwargs)
            lf = pl.scan_delta(dt, use_pyarrow=True)
        return lf
