from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing import FilePath
    from pfeed.data_models.base_data_model import BaseMetadataModel

import polars as pl
import pyarrow as pa

from pfeed._io.base_io import BaseIO
from pfeed.enums import Compression


class ParquetIO(BaseIO):
    FILE_EXTENSION: str = '.parquet'
    
    def __init__(
        self, 
        filesystem: pa_fs.FileSystem, 
        storage_options: dict,
        compression: Compression | str | None=Compression.SNAPPY, 
    ):
        super().__init__(
            filesystem=filesystem, 
            storage_options=storage_options, 
            compression=compression,
        )
    
    def exists(self, file_path: FilePath) -> bool:
        """Check if a valid parquet file exists at this path.
        
        Returns:
            True if file exists and is a valid parquet file
            False if file doesn't exist or is not valid parquet
        """
        if not self.exists(file_path):
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
    
    def write(self, data: pa.Table, file_path: FilePath, metadata: BaseMetadataModel | None=None):
        self._write_pyarrow_table_with_metadata(data, file_path, metadata=metadata)

    def read(self, file_paths: list[FilePath]) -> pl.LazyFrame | None:
        lf: pl.LazyFrame | None = None
        non_empty_file_paths = [file_path for file_path in file_paths if self.exists(file_path) and not self.is_empty(file_path)]
        if non_empty_file_paths:
            lf = pl.scan_parquet(non_empty_file_paths, storage_options=self._storage_options)
        return lf
