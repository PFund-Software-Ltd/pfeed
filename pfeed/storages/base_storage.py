from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfeed.types.core import tDataModel

from pathlib import Path
from dataclasses import dataclass, field

from pfeed.const.enums import DataStorage
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


@dataclass
class BaseStorage:
    name: DataStorage
    data_model: tDataModel
    data_path: Path | None = None
    file_path: Path | None = None
    # kwargs for the storage class, e.g. for MinIO, kwargs will be passed to Minio()
    kwargs: dict = field(default_factory=dict)
    
    def __post_init__(self):
        if isinstance(self.data_model, TimeBasedDataModel):
            assert not self.data_model.end_date, \
                'data storage is per date, only start_date should be provided, end_date must be None'
        self.data_path = self._create_data_path()
        self.file_path: Path | str = self._create_file_path()
        if isinstance(self.file_path, Path):
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
    
    @property
    def date(self) -> datetime.date:
        if hasattr(self.data_model, 'date'):
            return self.data_model.date
        else:
            raise ValueError(f'{type(self.data_model)} does not have a date attribute')
    
    @property
    def file_extension(self) -> str:
        return self.data_model.file_extension
    
    @property
    def filename(self) -> str:
        return self.data_model.filename
    
    @property
    def storage_path(self) -> Path:
        return self.data_model.storage_path
        
    @staticmethod
    def _create_data_path() -> Path:
        from pfeed.config import get_config
        config = get_config()
        return Path(config.data_path)
    
    def _create_file_path(self) -> Path | str:
        if isinstance(self.data_path, str):  # e.g. for MinIO, data_path is a string because s3:// doesn't work with pathlib
            return self.data_path + '/' + str(self.storage_path)
        elif isinstance(self.data_path, Path):
            return self.data_path / self.storage_path
        else:
            raise ValueError(f'{type(self.data_path)} is not supported')

    def exists(self) -> bool:
        return self.file_path.exists()

    def __str__(self):
        return f'{self.name}:{self.data_model}'
