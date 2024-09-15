from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import (
        tSUPPORTED_ENVIRONMENTS,
        tSUPPORTED_DOWNLOAD_DATA_SOURCES, 
    )
    
from dataclasses import dataclass, field
from pathlib import Path

from pfeed.resolution import ExtendedResolution
from pfeed.utils.utils import create_filename
from pfeed.const.paths import DATA_PATH


@dataclass
class FilePath:
    env: tSUPPORTED_ENVIRONMENTS
    data_source: tSUPPORTED_DOWNLOAD_DATA_SOURCES
    trading_venue: str
    pdt: str
    resolution: str | ExtendedResolution
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
        self.env = self.env.upper()
        self.data_source = self.data_source.upper()
        self.trading_venue = self.trading_venue.upper()
        if isinstance(self.resolution, str):
            self.resolution = ExtendedResolution(self.resolution)
        self.resolution = repr(self.resolution)
        self.pdt = self.pdt.upper()
        self.ptype = self.pdt.split('_')[2]
        self.date = str(self.date)
        self.year, self.month, self.day = self.date.split('-')
        
        # derived attributes
        self.filename = create_filename(self.pdt, self.date, self.file_extension)
        self.storage_path = str(
            Path(self.env)
            / self.data_source
            / self.trading_venue
            / self.ptype
            / self.pdt
            / self.resolution
            / self.year
            / self.month 
            / self.filename
        )
        self.file_path = str(self.data_Path / self.storage_Path)
        
    def exists(self):
        return self.file_Path.exists()
    
    def resolve(self):
        return self.file_Path.resolve()