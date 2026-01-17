from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed._io.file_io import FileIO

from abc import abstractmethod

from pfeed.enums import IOFormat, Compression
from pfeed.storages.base_storage import BaseStorage


class FileBasedStorage(BaseStorage):
    # FIXME: can't use it with DuckDBStorage, attach it based on io? with_io()?
    def __new__(cls, *args, use_deltalake: bool = False, **kwargs):
        if use_deltalake:
            # Create a new class that inherits from both BaseStorage and DeltaLakeStorageMixin
            from pfeed.storages.deltalake_storage_mixin import DeltaLakeStorageMixin

            new_cls = type(f"DeltaLake{cls.__name__}", (cls, DeltaLakeStorageMixin), {})
            return super(BaseStorage, new_cls).__new__(new_cls)
        return super().__new__(cls)

    @abstractmethod
    def get_filesystem(self) -> pa_fs.FileSystem:
        pass

    def with_io(
        self,
        io_format: IOFormat=IOFormat.PARQUET,
        compression: Compression | str | None=Compression.SNAPPY,
        io_options: dict | None = None,
    ) -> FileIO:
        return super().with_io(io_format, compression, io_options=io_options)

    def _create_io(
        self,
        io_format: IOFormat,
        compression: Compression | str | None,
        storage_options: dict | None = None,
        io_options: dict | None = None,
    ) -> FileIO:
        # EXTEND: add more file-based formats, iceberg, etc.
        assert io_format in [IOFormat.PARQUET, IOFormat.DELTALAKE], (
            f"File-based storage only supports {IOFormat.PARQUET} and {IOFormat.DELTALAKE} formats"
        )
        io_format = IOFormat[io_format.upper()]
        IO: type[FileIO] = io_format.io_class
        return IO(
            filesystem=self.get_filesystem(),
            compression=compression,
            storage_options=storage_options,
            io_options=io_options,
        )
