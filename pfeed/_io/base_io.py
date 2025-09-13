from __future__ import annotations
from typing_extensions import TypedDict
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.data_models.base_data_model import BaseFileMetadata
    from pfeed._typing import GenericData, tDataTool, FilePath
    class StorageMetadata(TypedDict, total=True):
        file_metadata: BaseFileMetadata
        missing_file_paths: list[FilePath]

from abc import ABC, abstractmethod

import pyarrow.fs as pa_fs


class BaseIO(ABC):
    def __init__(self, filesystem: pa_fs.FileSystem, compression: str='gzip', storage_options: dict | None=None):
        self._filesystem = filesystem
        self._compression = compression
        self._storage_options = storage_options
    
    @property
    def is_local_fs(self) -> bool:
        return isinstance(self._filesystem, pa_fs.LocalFileSystem)
    
    def _mkdir(self, file_path: FilePath):
        if self.is_local_fs:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        
    def _exists(self, file_path: FilePath) -> bool:
        file_info = self._filesystem.get_file_info(str(file_path).replace('s3://', ''))
        return file_info.type == pa_fs.FileType.File
        
    @abstractmethod
    def write(self, file_path: FilePath, data: GenericData, **kwargs):
        pass

    @abstractmethod
    def read(self, file_path: FilePath, data_tool: tDataTool='polars', **kwargs) -> GenericData | None:
        pass
