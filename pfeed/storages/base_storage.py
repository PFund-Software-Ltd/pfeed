from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Any, Literal, overload

if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseMetadata
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import GenericData, GenericFrame
    from pfeed.storages.database_storage import DatabaseURI
    from pfeed._io.base_io import BaseIO
    from pfeed.streaming.streaming_message import StreamingMessage

from abc import ABC, abstractmethod
from pprint import pformat

from pfeed.enums import DataLayer, IOFormat
from pfeed.utils.file_path import FilePath


class BaseStorage(ABC):
    SUPPORTED_IO_FORMATS: ClassVar[list[IOFormat]] = []
    
    def __init__(
        self,
        data_path: FilePath | DatabaseURI,
        data_layer: DataLayer,
        data_domain: str,
        storage_options: dict[str, Any] | None = None,
    ):
        '''
        Args:
            data_layer: Data layer to store the data.
            data_domain: Data domain of the data, used for grouping data inside a data layer.
            # NOTE: NOT IMPLEMENTED YET, storage_options should be compatible with other libraries like polars, etc.
            storage_options: Storage options
        '''
        self.data_path = data_path
        self.data_layer = DataLayer[str(data_layer).upper()]
        self.data_domain = data_domain.upper()
        self.storage_options: dict[str, Any] = storage_options or {}
        self._data_model: BaseDataModel | None = None
        self._data_handler: BaseDataHandler | None = None
        self._io: BaseIO | None = None
        self._is_data_handler_stale: bool = False
    
    @abstractmethod
    def _create_io(self, *args: Any, io_options: dict[str, Any] | None = None, **kwargs: Any) -> BaseIO:
        pass

    @property
    def name(self) -> str:
        return self.__class__.__name__
    
    def __str__(self) -> str:
        parts = []
        if self._io:
            parts.append(f"io={self._io.__class__.__name__}")
        if self._data_model:
            parts.append(f"data_model={self._data_model}")
        parts.extend(
            [
                f"data_path={self.data_path}",
                f"data_layer={self.data_layer}",
                f"data_domain={self.data_domain}",
            ]
        )
        if self.storage_options:
            parts.append(f"storage_options={self.storage_options}")
        return f"{self.__class__.__name__} (" + " | ".join(parts) + ")"
    
    def __repr__(self) -> str:
        data = {
            "data_path": self.data_path,
            "data_layer": self.data_layer,
            "data_domain": self.data_domain,
            "storage_options": self.storage_options,
            "data_model": str(self._data_model),
            "io": self._io.name,
            "data_handler": self._data_handler,
        }
        return f"{self.name}(\n{pformat(data, sort_dicts=False)}\n)"

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
                f"No data handler has been set for storage: {self.name}, please call with_data_model() first"
            )
        return self._data_handler

    @property
    def io(self) -> BaseIO:
        if not self._io:
            raise AttributeError(f"No IO has been set for storage: {self.name}")
        return self._io

    def with_data_model(self, data_model: BaseDataModel) -> BaseStorage:
        self._data_model = data_model
        self._is_data_handler_stale = True
        return self

    def with_io(self, *, io_options: dict[str, Any] | None = None, **kwargs: Any) -> BaseStorage:
        self._io = self._create_io(io_options=io_options, **kwargs)
        self._is_data_handler_stale = True
        return self

    def _initialize_data_handler(self) -> None:
        DataHandler: type[BaseDataHandler] = self.data_model.data_handler_class
        self._data_handler = DataHandler(
            data_path=self.data_path,
            data_layer=self.data_layer,
            data_domain=self.data_domain,
            data_model=self.data_model,
            io=self.io,
        )

    def write_data(
        self, data: GenericData | StreamingMessage, streaming: bool = False, **io_kwargs: Any
    ):
        self.data_handler.write(data=data, streaming=streaming, **io_kwargs)
    
    @overload
    def read_data(self, include_metadata: Literal[True], **io_kwargs: Any) -> tuple[GenericFrame | None, BaseMetadata]: ...
    
    @overload
    def read_data(self, include_metadata: Literal[False] = ..., **io_kwargs: Any) -> GenericFrame | None: ...

    def read_data(self, include_metadata: bool = False, **io_kwargs: Any) -> GenericFrame | None | tuple[GenericFrame | None, BaseMetadata]:
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
        if include_metadata:
            metadata: BaseMetadata = self.data_handler.read_metadata()
            return data, metadata
        else:
            return data
