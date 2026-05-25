from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import SourcePath

    # MetadataModel (e.g. TimeBasedMetadataModel) in dict format
    MetadataDict: TypeAlias = dict[str, Any]


from abc import ABC, abstractmethod


class BaseIO(ABC):
    SUPPORTS_PARALLEL_WRITES: bool = (
        False  # if supports parallel writes to the same destination
    )
    DATE_FILTER_PREDICATE: str = ""
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
    def read_metadata(
        self, *args: Any, **kwargs: Any
    ) -> dict[SourcePath, MetadataDict]:
        pass

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return None

    @classmethod
    def is_file_io(cls, strict: bool = True) -> bool:
        """Check if the IO is a FileIO.

        Args:
            strict: If True, only returns True if FileIO is the first parent class.
                If False, returns True if FileIO is anywhere in the inheritance chain.
                e.g. DuckDBIO(DatabaseIO, FileIO) -> strict=True returns False, strict=False returns True.
        """
        from pfeed._io.file_io import FileIO

        if strict:
            return cls.__bases__[0] is FileIO
        else:
            return issubclass(cls, FileIO)

    @classmethod
    def is_table_io(cls, strict: bool = True) -> bool:
        """Check if the IO is a TableIO.

        Args:
            strict: If True, only returns True if TableIO is the first parent class.
                If False, returns True if TableIO is anywhere in the inheritance chain.
        """
        from pfeed._io.table_io import TableIO

        if strict:
            return cls.__bases__[0] is TableIO
        else:
            return issubclass(cls, TableIO)

    @classmethod
    def is_database_io(cls, strict: bool = True) -> bool:
        """Check if the IO is a DatabaseIO.

        Args:
            strict: If True, only returns True if DatabaseIO is the first parent class.
                If False, returns True if DatabaseIO is anywhere in the inheritance chain.
        """
        from pfeed._io.database_io import DatabaseIO

        if strict:
            return cls.__bases__[0] is DatabaseIO
        else:
            return issubclass(cls, DatabaseIO)
