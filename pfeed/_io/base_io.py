from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed._typing import GenericData
    from pfeed._typing import tDataTool

from abc import ABC, abstractmethod
from pathlib import Path

import pyarrow.fs as pa_fs


class BaseIO(ABC):
    def __init__(self, filesystem: pa_fs.FileSystem, compression: str='gzip', storage_options: dict | None=None):
        self._filesystem = filesystem
        self._compression = compression
        self._storage_options = storage_options
    
    @property
    def is_local_fs(self) -> bool:
        return isinstance(self._filesystem, pa_fs.LocalFileSystem)
    
    def _mkdir(self, file_path: Path):
        if self.is_local_fs:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        
    def _exists(self, file_path: Path) -> bool:
        file_info = self._filesystem.get_file_info(str(file_path).replace('s3://', ''))
        return file_info.type == pa_fs.FileType.File
        
    @abstractmethod
    def write(self, file_path: Path, data: GenericData, **kwargs):
        pass

    @abstractmethod
    def read(self, file_path: Path, data_tool: tDataTool='polars', **kwargs) -> GenericData | None:
        pass
