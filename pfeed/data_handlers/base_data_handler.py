from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.typing import GenericData, FilePath
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed._io.base_io import BaseIO, MetadataModelAsDict

from abc import ABC, abstractmethod
from pathlib import Path
from pydantic import BaseModel

from pfeed.enums import DataTool
from pfeed.data_models.base_data_model import BaseMetadataModel


class BaseMetadata(BaseModel):
    file_metadatas: dict[FilePath, BaseMetadataModel]
    missing_file_paths: list[FilePath]


class BaseDataHandler(ABC):
    def __init__(self, data_model: BaseDataModel, data_path: FilePath, io: BaseIO):
        self._data_model = data_model
        self._data_path = data_path
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
    def _create_filename(self, *args, **kwargs) -> str:
        pass

    @abstractmethod
    def _create_storage_path(self, *args, **kwargs) -> Path:
        pass
    
    @abstractmethod
    def _create_file_path(self, *args, **kwargs) -> FilePath:
        pass

    @abstractmethod
    def create_file_paths(self) -> list[FilePath]:
        pass

    def _read_file_metadatas(self, file_paths: list[FilePath]) -> dict[FilePath, BaseMetadataModel]:
        metadata_dict: dict[FilePath, MetadataModelAsDict] = self._io.read_metadata(file_paths=file_paths)
        MetadataModel: type[BaseMetadataModel] = self._data_model.metadata_class
        file_metadatas = {
            file_path: MetadataModel.from_dict(file_metadata)
            for file_path, file_metadata in metadata_dict.items()
        }
        return file_metadatas

    def read_metadata(self) -> BaseMetadata:
        file_paths = self.create_file_paths()
        return BaseMetadata(
            file_metadatas=self._read_file_metadatas(file_paths=file_paths),
            missing_file_paths=[fp for fp in file_paths if not self._io.exists(fp)],
        )

    def _is_using_streaming_io(self) -> bool:
        return self._io.SUPPORTS_STREAMING

    def _is_using_table_format(self) -> bool:
        return self._io.IS_TABLE_FORMAT

    def _get_file_extension(self) -> str:
        if self._io.FILE_EXTENSION is None:
            raise ValueError(f'{self._io.__class__.__name__} does not support file extension, cannot get file extension')
        return self._io.FILE_EXTENSION