from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Any, ClassVar, assert_never

if TYPE_CHECKING:
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.database_storage import DatabaseURI
    from pfeed._io.base_io import BaseIO, MetadataDict

from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict

from pfeed.enums import DataLayer, DataSource, IOType
from pfeed.utils.file_path import FilePath
from pfeed._io.table_io import TablePath
from pfeed._io.database_io import DBPath
SourcePath: TypeAlias = FilePath | TablePath | DBPath


class BaseDataMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    data_source: DataSource
    data_origin: str = ""


class BaseDataHandler(ABC):
    metadata_class: ClassVar[type[BaseDataMetadata]]

    def __init__(self, data_path: FilePath | DatabaseURI, data_layer: DataLayer, data_domain: str, data_model: BaseDataModel, io: BaseIO):
        self._data_path = data_path
        self._data_layer = data_layer
        self._data_domain = data_domain
        self._data_model = data_model
        self._io: BaseIO = io
        self._file_paths: list[FilePath] = []
        self._table_path: TablePath | None = self._create_table_path() if self._is_table_io() else None
        self._db_path: DBPath | None = self._create_db_path() if self._is_database_io() else None
        if self._is_file_io():
            self._io_type = IOType.FILE
        elif self._is_table_io():
            self._io_type = IOType.TABLE
        elif self._is_database_io():
            self._io_type = IOType.DATABASE
        else:
            raise ValueError(f"Unsupported IO type: {self._io}")

    @abstractmethod
    def write(self, data: Any, *args: Any, **kwargs: Any):
        pass

    @abstractmethod
    def read(self, **kwargs: Any) -> Any | None:
        pass

    @abstractmethod
    def _validate_schema(self, data: Any) -> Any:
        pass

    @abstractmethod
    def _create_file_path(self, *args: Any, **kwargs: Any) -> FilePath:
        pass

    @abstractmethod
    def _create_table_path(self, *args: Any, **kwargs: Any) -> TablePath:
        pass

    @abstractmethod
    def _create_db_path(self, *args: Any, **kwargs: Any) -> DBPath:
        pass

    @abstractmethod
    def _create_metadata(self, *args: Any, **kwargs: Any) -> BaseDataMetadata:
        pass

    def _is_streaming_io(self) -> bool:
        return self._io.SUPPORTS_STREAMING

    def _is_file_io(self, strict: bool = True) -> bool:
        """Check if the IO is a FileIO.

        Args:
            strict: If True, only returns True if FileIO is the first parent class.
                If False, returns True if FileIO is anywhere in the inheritance chain.
                e.g. DuckDBIO(DatabaseIO, FileIO) -> strict=True returns False, strict=False returns True.
        """
        from pfeed._io.file_io import FileIO
        if strict:
            return type(self._io).__bases__[0] is FileIO
        else:
            return isinstance(self._io, FileIO)

    def _is_table_io(self, strict: bool = True) -> bool:
        """Check if the IO is a TableIO.

        Args:
            strict: If True, only returns True if TableIO is the first parent class.
                If False, returns True if TableIO is anywhere in the inheritance chain.
        """
        from pfeed._io.table_io import TableIO
        if strict:
            return type(self._io).__bases__[0] is TableIO
        else:
            return isinstance(self._io, TableIO)

    def _is_database_io(self, strict: bool = True) -> bool:
        """Check if the IO is a DatabaseIO.

        Args:
            strict: If True, only returns True if DatabaseIO is the first parent class.
                If False, returns True if DatabaseIO is anywhere in the inheritance chain.
        """
        from pfeed._io.database_io import DatabaseIO
        if strict:
            return type(self._io).__bases__[0] is DatabaseIO
        else:
            return isinstance(self._io, DatabaseIO)

    def _get_file_extension(self) -> str:
        if self._io.FILE_EXTENSION is None:
            raise ValueError(f'{self._io.__class__.__name__} does not have a file extension')
        return self._io.FILE_EXTENSION

    def _supports_partitioning(self) -> bool:
        return self._io.SUPPORTS_PARTITIONING

    def _supports_parallel_writes(self) -> bool:
        return self._io.SUPPORTS_PARALLEL_WRITES

    def read_metadata(self) -> dict[SourcePath, BaseDataMetadata]:
        match self._io_type:
            case IOType.FILE:
                source_paths = self._file_paths
            case IOType.TABLE:
                source_paths = self._table_path
            case IOType.DATABASE:
                source_paths = self._db_path
            case _:
                assert_never(self._io_type)
        metadata_dict: dict[SourcePath, MetadataDict] = self._io.read_metadata(source_paths)
        Metadata = self.metadata_class
        metadata = {
            source_path: Metadata(**metadata_value)
            for source_path, metadata_value in metadata_dict.items()
        }
        return metadata

    def find_missing_source_paths(self):
        match self._io_type:
            case IOType.FILE:
                missing_source_paths=[fp for fp in self._file_paths if not self._io.exists(fp)]
            case (IOType.TABLE | IOType.DATABASE) as io_type:
                source_path = self._table_path if io_type == IOType.TABLE else self._db_path
                assert source_path is not None, f'source_path is not set for {self._io.name}'
                missing_source_paths = [source_path] if not self._io.exists(source_path) else []
            case _:
                assert_never(self._io_type)
        return missing_source_paths
