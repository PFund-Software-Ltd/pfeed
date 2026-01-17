from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseMetadata
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import GenericData, GenericFrame
    from pfeed._io.base_io import BaseIO
    from pfeed.messaging.streaming_message import StreamingMessage

from abc import ABC, abstractmethod
from pathlib import Path

from pfeed.config import get_config
from pfeed.enums import DataLayer, DataStorage


config = get_config()


class BaseStorage(ABC):
    name: ClassVar[DataStorage]
    
    def __init__(
        self,
        data_layer: DataLayer,
        data_path: Path | None = None,
        storage_options: dict | None = None,
    ):
        self.data_path = data_path or config.data_path
        self.data_layer = DataLayer[str(data_layer).upper()]
        self.storage_options = storage_options or {}
        self._data_model: BaseDataModel | None = None
        self._data_handler: BaseDataHandler | None = None
        self._io: BaseIO | None = None
        self._is_data_handler_stale: bool = False
    
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if 'name' not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must define 'name' class attribute, e.g. name = DataStorage.LOCAL")

    @abstractmethod
    def _create_io(
        self,
        *args,
        storage_options: dict | None = None,
        io_options: dict | None = None,
        **kwargs,
    ):
        pass

    @property
    def data_model(self) -> BaseDataModel:
        if not self._data_model:
            raise AttributeError(f"No data model has been set for storage: {self.name}")
        return self._data_model

    @property
    def data_handler(self) -> BaseDataHandler:
        # re-initialize data handler if data model or io has changed
        if self._is_data_handler_stale:
            self._initialize_data_handler()
            self._is_data_handler_stale = False
        if not self._data_handler:
            raise AttributeError(
                f"No data handler has been set for storage: {self.name}"
            )
        return self._data_handler

    @property
    def io(self) -> BaseIO:
        if not self._io:
            raise AttributeError(f"No IO has been set for storage: {self.name}")
        return self._io

    def __str__(self):
        if self._data_model:
            return f"{self.name}:{self._data_model}"
        else:
            return f"{self.name}"

    def with_data_model(self, data_model: BaseDataModel) -> BaseStorage:
        self._data_model = data_model
        self._is_data_handler_stale = True
        return self

    def with_io(self, *args, io_options: dict | None = None, **kwargs) -> BaseStorage:
        self._io = self._create_io(
            *args,
            storage_options=self.storage_options,
            io_options=io_options,
            **kwargs,
        )
        self._is_data_handler_stale = True
        return self

    def _initialize_data_handler(self) -> None:
        DataHandler: type[BaseDataHandler] = self.data_model.data_handler_class
        self._data_handler = DataHandler(
            data_path=self.data_path,
            data_layer=self.data_layer,
            data_model=self.data_model,
            io=self.io,
        )

    def write_data(
        self, data: GenericData | StreamingMessage, streaming: bool = False, **io_kwargs
    ):
        self.data_handler.write(
            data=data,
            streaming=streaming,
            validate=self.data_layer != DataLayer.RAW,
            **io_kwargs,
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
