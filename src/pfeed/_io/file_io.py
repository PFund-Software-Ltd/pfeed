# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import polars as pl
    from pyarrow.parquet import FileMetaData as PyArrowParquetFileMetaData

    from pfeed._io.base_io import MetadataDict
    from pfeed.data_handlers.base_data_handler import BaseDataMetadata
    from pfeed.enums import Compression
    from pfeed.utils.file_path import FilePath

import json
from abc import ABC, abstractmethod

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from pfeed._io.base_io import BaseIO
from pfeed.enums import Compression


class FileIO(BaseIO, ABC):
    # when it's empty, it means metadata is stored alongside the data file
    # when it's not empty, it means metadata is stored in a separate file
    METADATA_FILENAME: str = ""

    def __init__(
        self,
        storage_options: dict[str, Any] | None = None,
        connect_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        write_options: dict[str, Any] | None = None,
        filesystem: pa_fs.FileSystem | None = None,
        compression: Compression | str = Compression.SNAPPY,
        **kwargs: Any,  # for compatibility with other IO classes
    ):
        """
        Args:
            storage_options: Filesystem-level credentials/config (e.g. S3 keys, endpoint, region).
                Forwarded to readers/writers that hit the underlying filesystem
                (e.g. `pl.scan_parquet(storage_options=...)`). Ignored for local filesystem.
            connect_options: Kwargs for opening a database connection. Not used by pure
                file-based IO; accepted only for interface symmetry with `DatabaseIO`.
            read_options: Kwargs forwarded to the subclass's read call
                (e.g. `pl.scan_parquet(**read_options)`).
            write_options: Kwargs forwarded to the subclass's write call
                (e.g. `pq.write_table(**write_options)`).
            filesystem: PyArrow filesystem used for IO. Defaults to `LocalFileSystem`.
            compression: Parquet compression codec applied when writing.
        """
        super().__init__(
            storage_options=storage_options,
            connect_options=connect_options,
            read_options=read_options,
            write_options=write_options,
        )
        self._filesystem = filesystem or pa_fs.LocalFileSystem()
        self._compression = Compression[compression.upper()]

    @abstractmethod
    def write(self, data: pa.Table, file_path: FilePath, *args: Any, **kwargs: Any):
        pass

    @abstractmethod
    def read(
        self, file_paths: list[FilePath], *args: Any, **kwargs: Any
    ) -> pl.LazyFrame | None:
        pass

    def exists(self, file_path: FilePath, *args: Any, **kwargs: Any) -> bool:
        """Check if a file exists at this path."""
        file_info = self._filesystem.get_file_info(file_path.schemeless)
        return file_info.type == pa_fs.FileType.File

    @abstractmethod
    def is_empty(self, file_path: FilePath, *args: Any, **kwargs: Any) -> bool:
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

    def write_metadata(self, table: pa.Table, metadata: BaseDataMetadata) -> pa.Table:
        """This only writes metadata to the table schema, not to the file.
        You must call _write_pyarrow_table to actually write the metadata to the file.
        """
        metadata_json = json.dumps(metadata.model_dump(mode="json") if metadata else {})
        schema = table.schema.with_metadata({b"metadata_json": metadata_json})
        return table.replace_schema_metadata(schema.metadata)

    def _write_pyarrow_table(
        self, table: pa.Table, file_path: FilePath, **io_kwargs: Any
    ):
        self._mkdir(file_path)
        with self._filesystem.open_output_stream(file_path.schemeless) as f:
            pq.write_table(table, f, compression=self._compression, **io_kwargs)

    def read_metadata(self, file_paths: list[FilePath]) -> dict[FilePath, MetadataDict]:
        """Read custom application metadata embedded in parquet schema."""
        metadata: dict[FilePath, MetadataDict] = {}

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
