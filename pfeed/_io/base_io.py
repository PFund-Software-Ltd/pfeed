from __future__ import annotations
from types import TracebackType
from typing import TYPE_CHECKING, TypeAlias, Any

if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import SourcePath
    # MetadataModel (e.g. TimeBasedMetadataModel) in dict format
    MetadataDict: TypeAlias = dict[str, Any]


from abc import ABC, abstractmethod


class BaseIO(ABC):
    SUPPORTS_STREAMING: bool = False
    SUPPORTS_PARALLEL_WRITES: bool = False  # if supports parallel writes to the same destination
    DATE_FILTER_PREDICATE: str = ''
    FILE_EXTENSION: str | None = None
    SUPPORTS_PARTITIONING: bool = False

    def __init__(
        self,
        storage_options: dict[str, Any] | None = None,
        connect_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        write_options: dict[str, Any] | None = None,
    ):
        self._storage_options: dict[str, Any] = storage_options or {}
        self._connect_options: dict[str, Any] = connect_options or {}
        self._read_options: dict[str, Any] = read_options or {}
        self._write_options: dict[str, Any] = write_options or {}

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def write(self, *args: Any, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def read(self, *args: Any, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    def exists(self, *args: Any, **kwargs: Any) -> bool:
        """Check if a file exists at this path."""
        pass

    @abstractmethod
    def is_empty(self, *args: Any, **kwargs: Any) -> bool:
        pass

    @abstractmethod
    def write_metadata(self, *args: Any, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def read_metadata(self, *args: Any, **kwargs: Any) -> dict[SourcePath, MetadataDict]:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None:
        pass  # No-op - do nothing
