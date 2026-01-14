from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Any

if TYPE_CHECKING:
    import json
    import polars as pl
    from pyarrow.parquet import FileMetaData as PyArrowParquetFileMetaData
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from pfeed.utils.file_path import FilePath

import time
import random
from abc import ABC, abstractmethod

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from pfeed.enums import Compression


# MetadataModel (e.g. TimeBasedMetadataModel) in dict format
MetadataModelAsDict: TypeAlias = dict[str, Any]


class BaseIO(ABC):
    IS_TABLE_FORMAT: bool = False
    SUPPORTS_STREAMING: bool = False
    SUPPORTS_PARTITIONING: bool = False
    FILE_EXTENSION: str | None = None
    # when it's empty, it means metadata is stored alongside the data file
    # when it's not empty, it means metadata is stored in a separate file
    METADATA_FILENAME: str = ""

    def __init__(
        self,
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(),
        storage_options: dict | None = None,
        compression: Compression | str | None = None,
        **kwargs,
    ):
        '''
        Args:
            filesystem: filesystem to use
            storage_options: storage options to use
            compression: compression to use
            kwargs: kwargs for the IO class
                e.g. for LanceDBIO, it's the same as the kwargs for lancedb.connect()
        '''
        self._filesystem = filesystem
        self._storage_options: dict = storage_options or {}
        self._compression = Compression[compression.upper()] if compression else None
        self._kwargs = kwargs

    @abstractmethod
    def write(self, file_path: FilePath, data: pa.Table, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, file_paths: list[FilePath], *args, **kwargs) -> pl.LazyFrame | None:
        pass

    def exists(self, file_path: FilePath, *args, **kwargs) -> bool:
        """Check if a file exists at this path."""
        file_info = self._filesystem.get_file_info(file_path.schemeless)
        return file_info.type == pa_fs.FileType.File

    @abstractmethod
    def is_empty(self, file_path: FilePath, *args, **kwargs) -> bool:
        pass

    @property
    def is_local_fs(self) -> bool:
        return isinstance(self._filesystem, pa_fs.LocalFileSystem)

    def _mkdir(self, file_path: FilePath):
        if self.is_local_fs:
            file_path.parent.mkdir(parents=True, exist_ok=True)

    # === Parquet Metadata Helpers ===
    # Shared methods for reading/writing metadata using parquet format.
    # Used by all IO classes regardless of their primary data format.
    def _get_pyarrow_file_metadata(
        self, file_path: FilePath
    ) -> PyArrowParquetFileMetaData:
        """Get file metadata created by pyarrow (e.g. rows, schema, row groups, etc.)."""
        return pq.read_metadata(file_path.schemeless, filesystem=self._filesystem)

    def _write_pyarrow_table_with_metadata(
        self,
        table: pa.Table,
        file_path: FilePath,
        metadata: BaseMetadataModel | None = None,
    ):
        self._mkdir(file_path)
        metadata = metadata.model_dump() if metadata else {}
        metadata_json = json.encode(metadata)
        schema = table.schema.with_metadata({b"metadata_json": metadata_json})
        table = table.replace_schema_metadata(schema.metadata)
        with self._filesystem.open_output_stream(file_path.schemeless) as f:
            pq.write_table(table, f, compression=self._compression)

    def read_metadata(
        self,
        file_paths: list[FilePath],
        max_retries: int=5,
        base_delay: float=0.1,
    ) -> dict[FilePath, MetadataModelAsDict]:
        """Read custom application metadata embedded in parquet schema.

        Handles race condition for table formats where metadata files may not exist immediately
        after table creation. This occurs when concurrent writers race:
        - Writer A creates the table structure
        - Writer B sees the table exists and tries to read metadata
        - Writer A hasn't finished writing the metadata file yet → FileNotFoundError

        Solution: Retry with exponential backoff, waiting for the write to complete.

        Args:
            file_paths: Paths to metadata files to read.
            max_retries: Maximum number of retry attempts for each file.
            base_delay: Initial delay in seconds, doubles with each retry (exponential backoff).

        Returns:
            Dictionary mapping file paths to their metadata.
        """
        metadata: dict[FilePath, MetadataModelAsDict] = {}

        for file_path in file_paths:
            for attempt in range(max_retries):
                try:
                    with self._filesystem.open_input_file(file_path.schemeless) as f:
                        parquet_file = pq.ParquetFile(f)
                        parquet_file_metadata = parquet_file.schema.to_arrow_schema().metadata
                        if b"metadata_json" in parquet_file_metadata:
                            metadata_json = parquet_file_metadata[b"metadata_json"]
                            metadata[file_path] = json.decode(metadata_json)
                    break  # Success, move to next file
                except FileNotFoundError as e:
                    if attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        delay = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
                        time.sleep(delay)
                    else:
                        # After all retries, re-raise the exception
                        raise e

        return metadata
