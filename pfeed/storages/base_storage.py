from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseMetadata
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import tStorage, tDataLayer, GenericData, GenericFrame
    from pfeed._io.base_io import BaseIO
    from pfeed.messaging.streaming_message import StreamingMessage
    from pfeed.streaming_settings import StreamingSettings

from abc import ABC, abstractmethod
from pathlib import Path

from pfeed.enums import DataLayer, DataStorage, FileFormat, Compression


class BaseStorage(ABC):
    def __new__(cls, *args, use_deltalake: bool = False, **kwargs):
        if use_deltalake:
            # Create a new class that inherits from both BaseStorage and DeltaLakeStorageMixin
            from pfeed.storages.deltalake_storage_mixin import DeltaLakeStorageMixin

            new_cls = type(f"DeltaLake{cls.__name__}", (cls, DeltaLakeStorageMixin), {})
            return super(BaseStorage, new_cls).__new__(new_cls)
        return super().__new__(cls)

    def __init__(
        self,
        name: tStorage,
        data_layer: tDataLayer,
        data_domain: str,
        base_data_path: Path | None = None,
        storage_options: dict | None = None,
        **storage_kwargs,
    ):
        from pfeed.config import get_config

        self.name = DataStorage[name.upper()]
        self.base_data_path = base_data_path or get_config().data_path
        self.data_layer = (
            DataLayer[data_layer.upper()]
            if not isinstance(data_layer, DataLayer)
            else data_layer
        )
        self.data_domain = data_domain.upper()
        self._data_model: BaseDataModel | None = None
        self._data_handler: BaseDataHandler | None = None
        self._storage_options = storage_options or {}
        self._storage_kwargs = storage_kwargs

    @classmethod
    def from_data_model(
        cls,
        data_model: BaseDataModel,
        data_layer: tDataLayer,
        data_domain: str,
        base_data_path: Path | None = None,
        storage_options: dict | None = None,
        **storage_kwargs,
    ) -> BaseStorage:
        instance = cls(
            data_layer=data_layer,
            data_domain=data_domain,
            base_data_path=base_data_path,
            storage_options=storage_options,
            **storage_kwargs,
        )
        instance.set_data_model(data_model)
        return instance

    @abstractmethod
    def get_filesystem(self) -> pa_fs.FileSystem:
        pass

    @property
    def data_model(self) -> BaseDataModel:
        if not self._data_model:
            raise AttributeError(f"No data model has been set for storage: {self.name}")
        return self._data_model

    @property
    def data_handler(self) -> BaseDataHandler:
        if not self._data_handler:
            # if not exists, create a data handler using default kwargs
            self._create_data_handler()
        return self._data_handler

    @property
    def data_path(self) -> Path:
        return (
            Path(self.base_data_path)
            / f"data_layer={self.data_layer}"
            / f"data_domain={self.data_domain}"
        )

    def __str__(self):
        if self._data_model:
            return f"{self.name}:{self._data_model}"
        else:
            return f"{self.name}"

    def set_data_model(self, data_model: BaseDataModel) -> None:
        """
        Set the data model for the storage.

        Args:
            data_model: data model is a detailed description of the data stored.
                if not provided, storage is just an empty vessel with basic properties, e.g. data_path,
                since it is not pointing to any data.
        """
        self._data_model = data_model

    def _create_data_handler(
        self,
        file_format: FileFormat | str = FileFormat.PARQUET,
        compression: Compression | str | None = Compression.SNAPPY,
        streaming_settings: StreamingSettings | None = None,
    ) -> None:
        from pfeed.utils.file_path import FilePath
        file_format = FileFormat[file_format.upper()]
        IO: type[BaseIO] = file_format.io_class
        io = IO(
            filesystem=self.get_filesystem(),
            storage_options=self._storage_options,
            compression=compression,
        )
        DataHandler: type[BaseDataHandler] = self.data_model.data_handler_class
        self._data_handler = DataHandler(
            data_model=self.data_model,
            data_path=FilePath(self.data_path),
            io=io,
            streaming_settings=streaming_settings,
        )

    def write_data(self, data: GenericData | StreamingMessage, streaming: bool = False):
        self.data_handler.write(
            data, streaming=streaming, validate=self.data_layer != DataLayer.RAW
        )

    def read_data(self, **io_kwargs) -> tuple[GenericFrame | None, BaseMetadata]:
        """Read data from storage.
    
        Args:
            **io_kwargs: Format-specific read options passed to the underlying IO implementation.
            
                For DeltaLake IO, for example, there are options like:
                    version (int | str | datetime | None): Delta table version to read. 
                        If None, reads the latest version.
                    storage_options (dict): Additional options passed to delta-rs.
                        See delta-rs documentation for available options.
                
                For Parquet IO:
                    No additional options currently supported.
        
        Returns:
            Tuple of (data, metadata)
        """
        data: GenericFrame | None = self.data_handler.read(**io_kwargs)
        metadata: BaseMetadata = self.data_handler.read_metadata()
        return data, metadata
