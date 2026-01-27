from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Any
if TYPE_CHECKING:
    from pfeed.typing import GenericData
    from pfeed.data_handlers.base_data_handler import SourcePath
    # MetadataModel (e.g. TimeBasedMetadataModel) in dict format
    MetadataModelAsDict: TypeAlias = dict[str, Any]

from abc import ABC, abstractmethod


class BaseIO(ABC):
    SUPPORTS_STREAMING: bool = False
    SUPPORTS_PARALLEL_WRITES: bool = False

    def __init__(self, storage_options: dict | None = None, io_options: dict | None = None):
        """
        Args:
            storage_options: storage options to use
            io_options: additional kwargs to pass to the IO class
        """
        self._storage_options: dict = storage_options or {}
        self._io_options: dict = io_options or {}
    
    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def write(self, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, *args, **kwargs) -> GenericData:
        pass

    @abstractmethod
    def exists(self, *args, **kwargs) -> bool:
        """Check if a file exists at this path."""
        pass

    @abstractmethod
    def is_empty(self, *args, **kwargs) -> bool:
        pass

    @abstractmethod
    def write_metadata(self, *args, **kwargs):
        pass

    @abstractmethod
    def read_metadata(self, *args, **kwargs) -> dict[SourcePath, MetadataModelAsDict]:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # No-op - do nothing