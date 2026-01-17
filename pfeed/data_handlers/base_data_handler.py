from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from pfeed.typing import GenericData
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed._io.base_io import BaseIO, MetadataModelAsDict
    from pfeed.utils.file_path import FilePath
    from pfeed._io.table_io import TablePath
    from pfeed._io.database_io import DBPath
    SourcePath: TypeAlias = FilePath | TablePath | DBPath

from pathlib import Path
from abc import ABC, abstractmethod
from pydantic import BaseModel

from pfeed.enums import DataTool, DataLayer
from pfeed.data_models.base_data_model import BaseMetadataModel


class BaseMetadata(BaseModel):
    source_metadata: dict[FilePath, BaseMetadataModel]
    missing_file_paths: list[FilePath]


class BaseDataHandler(ABC):
    def __init__(self, data_path: Path, data_layer: DataLayer, data_model: BaseDataModel, io: BaseIO):
        self._data_path = data_path
        self._data_layer = data_layer
        self._data_model = data_model
        self._io: BaseIO = io

    @abstractmethod
    def write(self, data: GenericData, *args, **kwargs):
        pass

    @abstractmethod
    def read(
        self, data_tool: DataTool = DataTool.polars, **kwargs
    ) -> GenericData | None:
        pass

    @abstractmethod
    def _validate_schema(self, data: GenericData) -> GenericData:
        pass

    @abstractmethod
    def _create_file_path(self, *args, **kwargs) -> FilePath:
        pass

    @abstractmethod
    def _create_table_path(self, *args, **kwargs) -> TablePath:
        pass

    @abstractmethod
    def _create_db_path(self, *args, **kwargs) -> DBPath:
        pass
    
    def get_source_path(self, *args, **kwargs) -> FilePath | TablePath | DBPath:
        if self._is_file_io():
            return self._create_file_path(*args, **kwargs)
        elif self._is_table_io():
            return self._create_table_path(*args, **kwargs)
        elif self._is_database_io():
            return self._create_db_path(*args, **kwargs)
        else:
            raise ValueError(f"Unsupported IO format: {self._io.name}")

    def _read_source_metadata(self, file_paths: list[FilePath]) -> dict[SourcePath, BaseMetadataModel]:
        if self._is_file_io():
            source_metadata_dict: dict[FilePath, MetadataModelAsDict] = self._io.read_metadata(file_paths=file_paths)
        elif self._is_table_io():
            pass
        elif self._is_database_io():
            pass
        else:
            raise ValueError(f'Unsupported IO format: {self._io.name}')
        MetadataModel: type[BaseMetadataModel] = self._data_model.metadata_class
        return {
            source: MetadataModel.from_dict(metadata)
            for source, metadata in source_metadata_dict.items()
        }

    def read_metadata(self) -> BaseMetadata:
        file_paths = self.create_file_paths()
        return BaseMetadata(
            source_metadata=self._read_source_metadata(file_paths=file_paths),
            missing_file_paths=[fp for fp in file_paths if not self._io.exists(fp)],
        )

    def _is_streaming_io(self) -> bool:
        return self._io.SUPPORTS_STREAMING

    def _is_file_io(self) -> bool:
        from pfeed._io.file_io import FileIO
        return isinstance(self._io, FileIO) and not self._is_table_io()
    
    def _is_table_io(self) -> bool:
        from pfeed._io.table_io import TableIO
        return isinstance(self._io, TableIO)

    def _is_database_io(self) -> bool:
        from pfeed._io.database_io import DatabaseIO
        return isinstance(self._io, DatabaseIO)

    def _get_file_extension(self) -> str:
        if self._io.FILE_EXTENSION is None:
            raise ValueError(f'{self._io.__class__.__name__} does not support file extension, cannot get file extension')
        return self._io.FILE_EXTENSION