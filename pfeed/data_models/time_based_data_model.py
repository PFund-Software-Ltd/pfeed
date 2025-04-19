from __future__ import annotations
from typing_extensions import TypedDict
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pathlib import Path
    import pyarrow.fs as pa_fs
    from pfeed.enums import DataLayer
    from pfeed.typing import tENVIRONMENT, tDATA_SOURCE
    
import datetime
from abc import abstractmethod

from pydantic import field_validator, Field, ValidationInfo

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class TimeBasedMetadata(TypedDict, total=True):
    env: tENVIRONMENT
    data_source: tDATA_SOURCE
    data_origin: str
    start_date: datetime.date
    end_date: datetime.date


# metadata for delta table
class TimeBasedDeltaMetadata(TypedDict, total=True):
    env: tENVIRONMENT
    data_source: tDATA_SOURCE
    data_origin: str
    dates: list[datetime.date]



class TimeBasedDataModel(BaseDataModel):
    start_date: datetime.date
    end_date: datetime.date = Field(description='Must be greater than or equal to start date.')

    @property
    def date(self) -> datetime.date:
        return self.start_date
    
    @field_validator('end_date')
    @classmethod
    def validate_end_date(cls, end_date: datetime.date, info: ValidationInfo) -> datetime.date:
        '''Validates the start and end dates of the data model.'''
        if info.data['start_date'] > end_date:
            raise ValueError(f'start date {info.data["start_date"]} must be before or equal to end date {end_date}.')
        return end_date
    
    @property
    def dates(self) -> list[datetime.date]:
        from pandas import date_range
        return date_range(self.start_date, self.end_date).date.tolist()

    def is_date_range(self) -> bool:
        return self.start_date != self.end_date

    def update_start_date(self, start_date: datetime.date) -> None:
        self.start_date = start_date
        
    def update_end_date(self, end_date: datetime.date) -> None:
        self.end_date = end_date
    
    def __str__(self):
        if self.start_date == self.end_date:
            return ':'.join([super().__str__(), str(self.start_date)])
        else:
            return ':'.join([super().__str__(), '(from)' + str(self.start_date), '(to)' + str(self.end_date)])
    
    @abstractmethod
    def create_filename(self, date: datetime.date) -> str:
        pass
    
    @abstractmethod
    def create_storage_path(self, date: datetime.date) -> Path:
        pass

    def create_data_handler(
        self, 
        data_layer: DataLayer,
        data_path: str,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None = None,
        use_deltalake: bool = False,                        
    ) -> TimeBasedDataHandler:
        return TimeBasedDataHandler(
            data_model=self, 
            data_layer=data_layer,
            data_path=data_path, 
            filesystem=filesystem, 
            storage_options=storage_options, 
            use_deltalake=use_deltalake
        )

    def to_metadata(self) -> dict:
        return {
            **super().to_metadata(),
            'start_date': self.start_date,
            'end_date': self.end_date,
        }