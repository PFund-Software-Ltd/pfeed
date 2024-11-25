from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfeed.types.core import tDataModel

from pathlib import Path
from dataclasses import dataclass, field

from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.const.enums import DataStorage


@dataclass
class BaseStorage:
    name: DataStorage
    data_model: tDataModel
    data_path: Path | None = None
    filename: str = ''
    file_extension: str = ''
    storage_path: Path | None = None
    file_path: Path | None = None
    # kwargs for the storage class, e.g. for MinIO, kwargs will be passed to Minio()
    kwargs: dict = field(default_factory=dict)
    
    def __post_init__(self):
        self.file_extension = self.data_model.file_extension
        self.data_path = self._create_data_path()
        if isinstance(self.data_model, MarketDataModel):
            self._init_using_market_data_model()
        else:
            raise ValueError(f'{type(self.data_model)} is not supported')
        if isinstance(self.file_path, Path):
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def _create_data_path() -> Path:
        from pfeed.config_handler import get_config
        config = get_config()
        return Path(config.data_path)
    
    @property
    def date(self) -> datetime.date:
        if hasattr(self.data_model, 'date'):
            return self.data_model.date
        else:
            raise ValueError(f'{type(self.data_model)} does not have a date attribute')
    
    def _init_using_market_data_model(self):
        date = str(self.data_model.date)
        year, month, day = date.split('-')
        self.filename = self.data_model.product + '_' + date + self.file_extension
        self.storage_path = (
            Path(self.data_model.env.value)
            / self.data_model.source.name
            / self.data_model.unique_identifier
            / self.data_model.product_type
            / self.data_model.product
            / str(self.data_model.resolution)
            / year
            / month 
            / self.filename
        )
        if isinstance(self.data_path, str):  # e.g. for MinIO, data_path is a string because s3:// doesn't work with pathlib
            self.file_path = self.data_path + '/' + str(self.storage_path)
        else:
            self.file_path = self.data_path / self.storage_path

    def exists(self) -> bool:
        return self.file_path.exists()

    def __str__(self):
        return f'{self.name} {self.data_model}'
