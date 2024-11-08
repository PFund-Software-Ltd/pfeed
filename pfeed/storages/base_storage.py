from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.core import tDataModel, tData

from abc import ABC, abstractmethod
from pathlib import Path
from dataclasses import dataclass

from pfeed.data_models.market_data_model import MarketDataModel


@dataclass
class BaseStorage(ABC):
    data_model: tDataModel
    data_path: Path | None = None
    filename: str = ''
    file_extension: str = ''
    storage_path: Path | None = None
    file_path: Path | None = None
    
    def __post_init__(self):
        self.file_extension = self.data_model.file_extension
        self.data_path = self._create_data_path()
        if isinstance(self.data_model, MarketDataModel):
            self._init_using_market_data_model()
        else:
            raise ValueError(f'{type(self.data_model)} is not supported')
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def _create_data_path() -> Path:
        from pfeed.config_handler import get_config
        config = get_config()
        return Path(config.data_path)
    
    def _init_using_market_data_model(self):
        date = str(self.data_model.date)
        self.year, self.month, self.day = date.split('-')
        self.filename = self.data_model.product + '_' + date + self.file_extension
        self.storage_path = (
            Path(self.data_model.env.value)
            / self.data_model.source.name
            / self.data_model.unique_identifier
            / self.data_model.product_type
            / self.data_model.product
            / str(self.data_model.resolution)
            / self.year
            / self.month 
            / self.filename
        )
        self.file_path = self.data_path / self.storage_path

    def exists(self) -> bool:
        return self.file_path.exists()
    
    @abstractmethod
    def load(self, data: tData, *args, **kwargs) -> None:
        pass
