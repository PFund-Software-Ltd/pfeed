from pathlib import Path

from typing import Literal

from pfeed.utils.utils import create_filename


class FilePath:
    '''Simple wrapper for file path to extract info faster'''
    def __init__(
            self, 
            data_path: str, 
            env: Literal['PAPER', 'LIVE'], 
            data_source: str, 
            data_type: Literal['raw', 'tick', 'second', 'minute', 'hour', 'daily'], 
            mode: Literal['historical', 'streaming'],
            pdt: str,
            date: str,
            file_extension: str,
        ):
        self.data_path = self.dpath = data_path
        self.data_Path = self.dPath = Path(data_path)
        self.env = env.lower()
        self.data_source = data_source.lower()
        self.data_type = self.dtype = data_type.lower()
        self.mode = mode
        self.pdt = pdt.lower()
        self.date = date
        self.file_extension = file_extension
        self.filename = create_filename(pdt.upper(), date, file_extension)
        self.storage_Path = self.sPath = Path(self.env) / self.data_source / self.mode / self.dtype / pdt.upper() / self.filename
        self.storage_path = self.spath = str(self.storage_Path)
        self.file_Path = self.fPath = self.data_Path / self.storage_Path
        self.file_path = self.fpath = str(self.file_Path)
        
    