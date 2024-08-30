from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DOWNLOAD_DATA_SOURCES, tSUPPORTED_DATA_MODES, tSUPPORTED_DATA_TYPES
    
from dataclasses import dataclass, field
from pathlib import Path

from pfeed.utils.utils import create_filename
from pfeed.const.paths import DATA_PATH


@dataclass
class FilePath:
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES
    mode: tSUPPORTED_DATA_MODES
    dtype: tSUPPORTED_DATA_TYPES
    pdt: str
    date: str
    file_extension: str = '.parquet.gz'
    data_path: str = str(DATA_PATH)
    # Derived attributes initialized via the `field(init=False)` to exclude them from the generated __init__ method
    filename: str = field(init=False)
    storage_path: str = field(init=False)
    file_path: str = field(init=False)
    
    @property
    def dpath(self):
        return self.data_path
    
    @property
    def data_Path(self):
        return Path(self.data_path)
    
    @property
    def dPath(self):
        return self.data_Path
    
    @property
    def spath(self):
        return self.storage_path
    
    @property
    def storage_Path(self):
        return Path(self.storage_path)
    
    @property
    def sPath(self):
        return self.storage_Path
    
    @property
    def fpath(self):
        return self.file_path
    
    @property
    def file_Path(self):
        return Path(self.file_path)
    
    @property
    def fPath(self):
        return self.file_Path
    
    @property
    def parent(self) -> Path:
        return self.file_Path.parent
    
    def __post_init__(self):
        self.data_source = self.data_source.lower()
        self.mode = self.mode.lower()
        self.dtype = self.dtype = self.dtype.lower()
        self.pdt = self.pdt.lower()
        self.date = str(self.date)
        
        # derived attributes
        self.filename = create_filename(self.pdt.upper(), self.date, self.file_extension)
        self.storage_path = str(Path(self.data_source) / self.mode / self.dtype / self.pdt.upper() / self.filename)
        self.file_path = str(self.data_Path / self.storage_Path)
        
    def exists(self):
        return self.file_Path.exists()
    
    def resolve(self):
        return self.file_Path.resolve()