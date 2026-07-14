# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pfeed._io.base_io import MetadataDict
    from pfeed.data_handlers.base_data_handler import BaseDataMetadata
    from pfeed.enums import Compression

import json
from abc import ABC

import pyarrow.fs as pa_fs

from pfeed._io.base_io import BaseIO
from pfeed.enums import Compression
from pfeed.utils.file_path import FilePath


class FileIO(BaseIO, ABC):
    METADATA_FILENAME: str = "metadata.json"
    # each artifact is written to its own unique path, so concurrent writes never
    # contend — safe under Ray. Format subclasses with shared destinations override.
    SUPPORTS_PARALLEL_WRITES: bool = True

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

    def write(self, data: Any, file_path: FilePath, *args: Any, **kwargs: Any) -> None:
        """Default file write: persist raw bytes verbatim.

        The generic floor — no (de)serialization, no format knowledge. Format-aware
        subclasses (ParquetIO, TableIO, ...) override with their own codec.
        """
        self._mkdir(file_path)
        with self._filesystem.open_output_stream(file_path.schemeless) as f:
            f.write(data)

    def read(self, file_paths: list[FilePath], *args: Any, **kwargs: Any) -> Any:
        """Default file read: return raw bytes from the first existing, non-empty path."""
        for file_path in file_paths:
            if self.exists(file_path) and not self.is_empty(file_path):
                with self._filesystem.open_input_file(file_path.schemeless) as f:
                    return f.read()
        return None

    def exists(self, file_path: FilePath, *args: Any, **kwargs: Any) -> bool:
        """Check if a file exists at this path."""
        file_info = self._filesystem.get_file_info(file_path.schemeless)
        return file_info.type == pa_fs.FileType.File

    def is_empty(self, file_path: FilePath, *args: Any, **kwargs: Any) -> bool:
        """Default emptiness check: missing or zero-byte. Format-aware subclasses
        (e.g. ParquetIO -> num_rows == 0) override with a semantic check."""
        file_info = self._filesystem.get_file_info(file_path.schemeless)
        return file_info.type == pa_fs.FileType.NotFound or file_info.size == 0

    @property
    def is_local_fs(self) -> bool:
        return isinstance(self._filesystem, pa_fs.LocalFileSystem)

    def _mkdir(self, file_path: FilePath):
        if self.is_local_fs:
            file_path.parent.mkdir(parents=True, exist_ok=True)

    def write_metadata(
        self, file_path: FilePath, metadata: BaseDataMetadata, *args: Any, **kwargs: Any
    ) -> None:
        metadata_path = FilePath(file_path.parent / self.METADATA_FILENAME)
        payload = (
            metadata.model_dump(mode="json", fallback=str)
            if metadata is not None
            else {}
        )
        self._mkdir(metadata_path)
        with self._filesystem.open_output_stream(metadata_path.schemeless) as f:
            f.write(json.dumps(payload, indent=2).encode())

    def read_metadata(
        self, file_paths: list[FilePath], *args: Any, **kwargs: Any
    ) -> dict[FilePath, MetadataDict]:
        metadata: dict[FilePath, MetadataDict] = {}
        for file_path in file_paths:
            metadata_path = FilePath(file_path.parent / self.METADATA_FILENAME)
            if FileIO.exists(self, metadata_path):
                with self._filesystem.open_input_file(metadata_path.schemeless) as f:
                    metadata[file_path] = json.loads(f.read())
        return metadata
