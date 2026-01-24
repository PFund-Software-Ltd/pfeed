from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.utils.file_path import FilePath

import polars as pl
import pyarrow as pa
import pyarrow.fs as pa_fs

from pfeed._io.file_io import FileIO
from pfeed.enums import Compression


class ParquetIO(FileIO):
    SUPPORTS_PARALLEL_WRITES: bool = False
    FILE_EXTENSION: str = '.parquet'
    
    def __init__(
        self, 
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(), 
        compression: Compression | str | None=Compression.SNAPPY, 
        storage_options: dict | None = None,
        io_options: dict | None = None,
    ):
        super().__init__(
            filesystem=filesystem, 
            compression=compression,
            storage_options=storage_options, 
            io_options=io_options,
        )
    
    def exists(self, file_path: FilePath) -> bool:
        """Check if a valid parquet file exists at this path.
        
        Returns:
            True if file exists and is a valid parquet file
            False if file doesn't exist or is not valid parquet
        """
        if not FileIO.exists(self, file_path):
            return False
        
        try:
            # Try to read parquet metadata to validate format
            self._get_pyarrow_file_metadata(file_path)
            return True
        except (pa.ArrowInvalid, Exception):
            # File exists but is not valid parquet
            return False
    
    def is_empty(self, file_path: FilePath) -> bool:
        return self._get_pyarrow_file_metadata(file_path).num_rows == 0
    
    def write(self, data: pa.Table, file_path: FilePath, **io_kwargs):
        self._write_pyarrow_table(data, file_path, **io_kwargs)

    def read(self, file_paths: list[FilePath], **io_kwargs) -> pl.LazyFrame | None:
        lf: pl.LazyFrame | None = None
        non_empty_file_paths = [file_path for file_path in file_paths if self.exists(file_path) and not self.is_empty(file_path)]
        if non_empty_file_paths:
            lf = pl.scan_parquet(non_empty_file_paths, storage_options=self._storage_options, **io_kwargs)
        return lf
