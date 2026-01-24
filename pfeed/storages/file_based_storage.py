from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pathlib import Path
    from pfeed._io.file_io import FileIO
    from pfeed._io.deltalake_io import DeltaLakeIO

from abc import abstractmethod

from pfeed.utils.file_path import FilePath
from pfeed.enums import IOFormat, Compression, DataLayer
from pfeed.storages.base_storage import BaseStorage


class FileBasedStorage(BaseStorage):
    # EXTEND: add more file-based formats, iceberg, etc.
    SUPPORTED_IO_FORMATS = [IOFormat.PARQUET, IOFormat.DELTALAKE]
    
    def __init__(
        self,
        data_path: Path | str,
        data_layer: DataLayer,
        data_domain: str,
        storage_options: dict | None = None,
    ):
        super().__init__(
            data_path=FilePath(data_path),
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )
    
    @abstractmethod
    def get_filesystem(self) -> pa_fs.FileSystem:
        pass

    def with_io(
        self,
        io_format: IOFormat=IOFormat.PARQUET,
        compression: Compression | None=Compression.SNAPPY,
        io_options: dict | None = None,
        **kwargs,  # Unused parameters accepted for compatibility with other storage backends
    ) -> FileIO | DeltaLakeIO:
        # Dynamically add DeltaLake mixin if using DeltaLake format
        if io_format == IOFormat.DELTALAKE:
            from pfeed.storages.deltalake_storage_mixin import DeltaLakeStorageMixin
            if not isinstance(self, DeltaLakeStorageMixin):
                new_cls = type(
                    f"DeltaLake{self.__class__.__name__}",
                    (self.__class__, DeltaLakeStorageMixin),
                    {'__module__': self.__class__.__module__}
                )
                self.__class__ = new_cls
        return super().with_io(io_format, compression, io_options=io_options)

    def _create_io(
        self,
        io_format: IOFormat,
        compression: Compression | None,
        storage_options: dict | None = None,
        io_options: dict | None = None,
    ) -> FileIO:
        assert io_format in self.SUPPORTED_IO_FORMATS, (
            f"File-based storage only supports IO formats: {self.SUPPORTED_IO_FORMATS}"
        )
        io_format = IOFormat[io_format.upper()]
        IO: type[FileIO] = io_format.io_class
        return IO(
            filesystem=self.get_filesystem(),
            compression=compression,
            storage_options=storage_options,
            io_options=io_options,
        )
