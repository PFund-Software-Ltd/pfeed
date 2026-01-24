from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import polars as pl
    from pyarrow.parquet import FileMetaData as PyArrowParquetFileMetaData
    from pfeed.enums import Compression
    from pfeed.utils.file_path import FilePath
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from pfeed._io.base_io import MetadataModelAsDict

import json
from abc import abstractmethod

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from pfeed.enums import Compression
from pfeed._io.base_io import BaseIO


class FileIO(BaseIO):
    FILE_EXTENSION: str | None = None
    SUPPORTS_PARTITIONING: bool = False
    # when it's empty, it means metadata is stored alongside the data file
    # when it's not empty, it means metadata is stored in a separate file
    METADATA_FILENAME: str = ""

    def __init__(
        self,
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(),
        compression: Compression | str = Compression.SNAPPY,
        storage_options: dict | None = None,
        io_options: dict | None = None,
    ):
        '''
        Args:
            filesystem: filesystem to use
            storage_options: storage options to use
            compression: compression to use
        '''
        super().__init__(storage_options=storage_options, io_options=io_options)
        self._filesystem = filesystem
        self._compression = Compression[compression.upper()]
    
    @abstractmethod
    def write(self, data: pa.Table, file_path: FilePath, *args, **kwargs):
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
    
    def write_metadata(self, table: pa.Table, metadata: BaseMetadataModel) -> pa.Table:
        '''This only writes metadata to the table schema, not to the file.
        You must call _write_pyarrow_table to actually write the metadata to the file.
        '''
        metadata = metadata.model_dump(mode='json') if metadata else {}
        metadata_json = json.dumps(metadata)
        schema = table.schema.with_metadata({b"metadata_json": metadata_json})
        return table.replace_schema_metadata(schema.metadata)
    
    def _write_pyarrow_table(self, table: pa.Table, file_path: FilePath, **io_options):
        self._mkdir(file_path)
        with self._filesystem.open_output_stream(file_path.schemeless) as f:
            pq.write_table(table, f, compression=self._compression, **io_options)

    def read_metadata(self, file_paths: list[FilePath]) -> dict[FilePath, MetadataModelAsDict]:
        """Read custom application metadata embedded in parquet schema."""
        metadata: dict[FilePath, MetadataModelAsDict] = {}
        
        for file_path in file_paths:
            if not FileIO.exists(self, file_path):
                continue
            with self._filesystem.open_input_file(file_path.schemeless) as f:
                parquet_file = pq.ParquetFile(f)
                parquet_file_metadata = parquet_file.schema.to_arrow_schema().metadata
                if b"metadata_json" in parquet_file_metadata:
                    metadata_json = parquet_file_metadata[b"metadata_json"]
                    metadata[file_path] = json.loads(metadata_json)
                    
        return metadata
