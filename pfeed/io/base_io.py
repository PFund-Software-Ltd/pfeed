from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing.core import tData
    from pfeed.typing.literals import tDATA_TOOL

from abc import ABC, abstractmethod
from pathlib import Path

import pyarrow.fs as pa_fs


class BaseIO(ABC):
    def __init__(self, file_system: pa_fs.FileSystem, compression: str='gzip', storage_options: dict | None=None):
        self._file_system = file_system
        self._compression = compression
        self._storage_options = storage_options
    
    @property
    def is_local_fs(self) -> bool:
        return isinstance(self._file_system, pa_fs.LocalFileSystem)
    
    def _mkdir(self, file_path: str):
        if self.is_local_fs:
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
    def _exists(self, file_path: str) -> bool:
        file_info = self._file_system.get_file_info(file_path)
        return file_info.type == pa_fs.FileType.File
        
    @abstractmethod
    def write(self, data: tData, file_path: str, metadata: dict | None=None, **kwargs):
        pass

    @abstractmethod
    def read(self, file_path: str, data_tool: tDATA_TOOL='pandas', **kwargs) -> tuple[tData, dict] | tData:
        pass
