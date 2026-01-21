from __future__ import annotations
from typing import ClassVar, TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler

import datetime

from pydantic import field_validator, Field, ValidationInfo

from pfeed.data_models.base_data_model import BaseDataModel, BaseMetadataModel


class TimeBasedMetadataModel(BaseMetadataModel):
    dates: list[datetime.date]


class TimeBasedDataModel(BaseDataModel):
    data_handler_class: ClassVar[type[TimeBasedDataHandler]]
    metadata_class: ClassVar[type[TimeBasedMetadataModel]]

    start_date: datetime.date = Field(description="Start of the date range.")
    end_date: datetime.date = Field(description="End of the date range. Must be greater than or equal to start date.")

    @property
    def date(self) -> datetime.date:
        assert not self.is_date_range(), 'start_date and end_date must be the same for a single date'
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
        assert isinstance(start_date, datetime.date), f'start_date must be a datetime.date, but got {type(start_date)}'
        self.start_date = start_date
        
    def update_end_date(self, end_date: datetime.date) -> None:
        assert isinstance(end_date, datetime.date), f'end_date must be a datetime.date, but got {type(end_date)}'
        self.end_date = end_date
    
    def __str__(self):
        if self.start_date == self.end_date:
            return ':'.join([super().__str__(), str(self.start_date)])
        else:
            return ':'.join([super().__str__(), '(from)' + str(self.start_date), '(to)' + str(self.end_date)])
    
    def to_metadata(self, **fields) -> TimeBasedMetadataModel:
        return super().to_metadata(
            dates=self.dates,
            **fields,
        )
