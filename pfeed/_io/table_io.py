from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias
if TYPE_CHECKING:
    import polars as pl
    from deltalake import DeltaTable
    from lancedb.table import LanceTable
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from pfeed._io.file_io import MetadataModelAsDict
    
    Table: TypeAlias = DeltaTable | LanceTable

import time
import random
from abc import abstractmethod

import pyarrow as pa
import pyarrow.fs as pa_fs

from pfeed.utils.file_path import FilePath
from pfeed._io.file_io import FileIO


class TablePath(FilePath):
    pass


class TableIO(FileIO):
    def exists(self, table_path: TablePath, *args, **kwargs) -> bool:
        file_info = self._filesystem.get_file_info(table_path.schemeless)
        return file_info.type == pa_fs.FileType.Directory

    @abstractmethod
    def is_empty(self, table_path: TablePath, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    def get_table(self, table_path: TablePath, *args, **kwargs) -> Table:
        pass
    
    @abstractmethod
    def write(self, table_path: TablePath, data: pa.Table, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, table_path: TablePath, *args, **kwargs) -> pl.LazyFrame | None:
        pass
    
    # NOTE: delta-rs, lancedb doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
    def write_metadata(self, table_path: TablePath, metadata: BaseMetadataModel):
        import pandas as pd
        empty_df_with_metadata = pd.DataFrame()
        table = pa.Table.from_pandas(empty_df_with_metadata, preserve_index=False)
        table_with_metadata = super().write_metadata(table, metadata)
        metadata_file_path = table_path / self.METADATA_FILENAME
        self._write_pyarrow_table(table_with_metadata, metadata_file_path)
    
    def read_metadata(
        self, 
        table_path: TablePath,
        max_retries: int=5,
        base_delay: float=0.1,
    ) -> dict[TablePath, MetadataModelAsDict]:
        """Read custom application metadata embedded in parquet schema.

        Handles race condition for table formats where metadata files may not exist immediately
        after table creation. This occurs when concurrent writers race:
        - Writer A creates the table structure
        - Writer B sees the table exists and tries to read metadata
        - Writer A hasn't finished writing the metadata file yet → FileNotFoundError

        Solution: Retry with exponential backoff, waiting for the write to complete.

        Args:
            table_path: Path to the table.
            max_retries: Maximum number of retry attempts for each file.
            base_delay: Initial delay in seconds, doubles with each retry (exponential backoff).

        Returns:
            Dictionary mapping file paths to their metadata.
        """
        metadata_file_path = table_path / self.METADATA_FILENAME
        for attempt in range(max_retries):
            try:
                file_metadata = super().read_metadata(file_paths=[metadata_file_path])
                if not file_metadata:
                    return {}
                else:
                    # use table_path as dict key
                    return { table_path: file_metadata[metadata_file_path] }
            except FileNotFoundError as e:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
                    time.sleep(delay)
                else:
                    # After all retries, re-raise the exception
                    raise e
