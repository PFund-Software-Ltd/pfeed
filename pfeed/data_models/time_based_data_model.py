from __future__ import annotations
from typing_extensions import TypedDict
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pathlib import Path
    import pyarrow.fs as pa_fs
    from pfeed.enums import DataLayer
    from pfund._typing import tEnvironment
    from pfeed._typing import tDataSource
    
import datetime
from abc import abstractmethod

from pydantic import field_validator, Field, ValidationInfo

from pfeed.data_models.base_data_model import BaseDataModel
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler
from pfeed.enums import StreamMode


class TimeBasedMetadata(TypedDict, total=True):
    env: tEnvironment
    data_source: tDataSource
    data_origin: str
    # REVIEW: is "date" already enough?
    start_date: datetime.date
    end_date: datetime.date


# metadata for delta table
class TimeBasedDeltaMetadata(TypedDict, total=True):
    env: tEnvironment
    data_source: tDataSource
    data_origin: str
    # FIXME: for consistency, turn it into "start_date" and "end_date"?
    dates: list[datetime.date]



class TimeBasedDataModel(BaseDataModel):
    start_date: datetime.date
    end_date: datetime.date = Field(description='Must be greater than or equal to start date.')
    data_handler_class: type[TimeBasedDataHandler] = TimeBasedDataHandler

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
    def create_filename(self, date: datetime.date, file_extension='.parquet') -> str:
        pass
    
    @abstractmethod
    def create_storage_path(self, date: datetime.date, use_deltalake: bool=False) -> Path:
        pass

    def create_data_handler(
        self, 
        data_layer: DataLayer,
        data_path: str,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None = None,
        use_deltalake: bool = False,
        stream_mode: StreamMode=StreamMode.FAST,
        delta_flush_interval: int=100,
    ) -> TimeBasedDataHandler:
        DataHandler: type[TimeBasedDataHandler] = self.data_handler_class
        return DataHandler(
            data_model=self, 
            data_layer=data_layer,
            data_path=data_path, 
            filesystem=filesystem, 
            storage_options=storage_options, 
            use_deltalake=use_deltalake,
            stream_mode=stream_mode,
            delta_flush_interval=delta_flush_interval,
        )

    def to_metadata(self) -> dict:
        return {
            **super().to_metadata(),
            'start_date': self.start_date,
            'end_date': self.end_date,
        }