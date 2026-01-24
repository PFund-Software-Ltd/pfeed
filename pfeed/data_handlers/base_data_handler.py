from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pfeed.typing import GenericData
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.storages.database_storage import DatabaseURI
    from pfeed._io.base_io import BaseIO, MetadataModelAsDict

from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict

from pfeed.enums import DataLayer
from pfeed.data_models.base_data_model import BaseMetadataModel

from pfeed.utils.file_path import FilePath
from pfeed._io.table_io import TablePath
from pfeed._io.database_io import DBPath
SourcePath: TypeAlias = FilePath | TablePath | DBPath


class BaseMetadata(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    source_metadata: dict[SourcePath, BaseMetadataModel]
    missing_source_paths: list[SourcePath]

    def to_dict(self) -> dict:
        return {
            "source_metadata": self.source_metadata,
            "missing_source_paths": self.missing_source_paths,
        }
    
    # VIBE-CODED
    def __str__(self) -> str:
        from pprint import pformat
        data = self.to_dict()
        lines = [f"{self.__class__.__name__}("]
        for key, value in data.items():
            if key == "source_metadata":
                lines.append(f"  {key}={{")
                items = list(value.items())
                for i, (source_path, metadata) in enumerate(items):
                    lines.append(f"    [{i}] '{source_path}':")
                    formatted = pformat(metadata, sort_dicts=False, indent=6)
                    lines.append(f"        {formatted.replace(chr(10), chr(10) + '        ')}")
                    if i < len(items) - 1:
                        lines.append("")  # blank line between entries
                lines.append("  }")
            elif isinstance(value, list):
                if not value:
                    lines.append(f"  {key}=[]")
                else:
                    lines.append(f"  {key}=[")
                    for item in value:
                        lines.append(f"    {item},")
                    lines.append("  ]")
            else:
                formatted = pformat(value, sort_dicts=False, indent=4)
                lines.append(f"  {key}={formatted}")
        lines.append(")")
        return "\n".join(lines)
    
    __repr__ = __str__


class BaseDataHandler(ABC):
    def __init__(self, data_path: FilePath | DatabaseURI, data_layer: DataLayer, data_domain: str, data_model: BaseDataModel, io: BaseIO):
        self._data_path = data_path
        self._data_layer = data_layer
        self._data_domain = data_domain
        self._data_model = data_model
        self._io: BaseIO = io
        self._file_paths: list[FilePath] = []
        self._table_path: TablePath | None = self._create_table_path() if self._is_table_io() else None
        self._db_path: DBPath | None = self._create_db_path() if self._is_database_io() else None
    
    @abstractmethod
    def write(self, data: GenericData, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, **kwargs) -> GenericData | None:
        pass

    @abstractmethod
    def _validate_schema(self, data: GenericData) -> GenericData:
        pass

    def _create_file_path(self, *args, **kwargs) -> FilePath:
        raise NotImplementedError(f'{self.__class__.__name__} is not implemented')

    def _create_table_path(self, *args, **kwargs) -> TablePath:
        raise NotImplementedError(f'{self.__class__.__name__} is not implemented')

    def _create_db_path(self, *args, **kwargs) -> DBPath:
        raise NotImplementedError(f'{self.__class__.__name__} is not implemented')
    
    def read_metadata(self) -> BaseMetadata:
        if self._is_file_io():
            source_metadata_dict: dict[FilePath, MetadataModelAsDict] = self._io.read_metadata(file_paths=self._file_paths)
            missing_source_paths=[fp for fp in self._file_paths if not self._io.exists(fp)]
        elif self._is_table_io():
            source_metadata_dict: dict[TablePath, MetadataModelAsDict] = self._io.read_metadata(table_path=self._table_path)
            missing_source_paths = [self._table_path] if not self._io.exists(self._table_path) else []
        elif self._is_database_io():
            source_metadata_dict: dict[DBPath, MetadataModelAsDict] = self._io.read_metadata(db_path=self._db_path)
            missing_source_paths = [self._db_path] if not self._io.exists(self._db_path) else []
        else:
            raise ValueError(f'Unsupported IO format: {self._io.name}')
        MetadataModel: type[BaseMetadataModel] = self._data_model.metadata_class
        source_metadata: dict[SourcePath, BaseMetadataModel] = {
            source_path: MetadataModel(**metadata_model_as_dict)
            for source_path, metadata_model_as_dict in source_metadata_dict.items()
        }
        return BaseMetadata(source_metadata=source_metadata, missing_source_paths=missing_source_paths)

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
            raise ValueError(f'{self._io.__class__.__name__} does not support file extension, cannot get file extension')
        return self._io.FILE_EXTENSION