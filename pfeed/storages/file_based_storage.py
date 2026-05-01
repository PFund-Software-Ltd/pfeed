from __future__ import annotations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed._io.io_config import IOConfig
    from pfeed._io.file_io import FileIO
    from pfeed._io.deltalake_io import DeltaLakeIO

from abc import abstractmethod

from pathlib import Path

from pfeed.utils.file_path import FilePath
from pfeed.enums import IOFormat, DataLayer
from pfeed.storages.base_storage import BaseStorage


class FileBasedStorage(BaseStorage):
    # EXTEND: add more file-based formats, iceberg, etc.
    SUPPORTED_IO_FORMATS = [IOFormat.PARQUET, IOFormat.DELTALAKE]
    
    def __init__(
        self,
        data_path: Path | str,
        data_layer: DataLayer = DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
        storage_options: dict[str, Any] | None = None,
        **kwargs: Any,  # additional kwargs for compatibility with other storages
    ):
        super().__init__(
            data_path=FilePath(data_path),
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
            **kwargs,
        )
    
    @abstractmethod
    def get_filesystem(self) -> pa_fs.FileSystem:
        pass

    def _get_io_kwargs(self) -> dict[str, Any]:
        '''Gets IO kwargs specific to the storage, e.g filesystem from a file-based storage'''
        return {
            'filesystem': self.get_filesystem(),
        }

    def with_io(self, io_config: IOConfig) -> FileIO | DeltaLakeIO:
        io_format = io_config.io_format
        assert io_format in self.SUPPORTED_IO_FORMATS, (
            f"File-based storage only supports IO formats: {self.SUPPORTED_IO_FORMATS}"
        )
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
        return super().with_io(io_config=io_config)
