import datetime
from abc import abstractmethod
from pathlib import Path

from pydantic import field_validator, Field, ValidationInfo

from pfeed.data_models.base_data_model import BaseDataModel


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
        # update filename and storage path to reflect the new start date
        self.filename = self._create_filename()
        self.storage_path = self._create_storage_path()
        
    def update_end_date(self, end_date: datetime.date) -> None:
        self.end_date = end_date
    
    def __str__(self):
        if self.start_date == self.end_date:
            return ':'.join([super().__str__(), str(self.start_date)])
        else:
            return ':'.join([super().__str__(), '(from)' + str(self.start_date), '(to)' + str(self.end_date)])

    def _create_filename(self, date: datetime.date | None=None) -> str:
        return self._create_filename_per_date(date=date or self.start_date)

    def _create_storage_path(self, date: datetime.date | None=None) -> Path:
        return self._create_storage_path_per_date(date=date or self.start_date)
    
    @abstractmethod
    def _create_filename_per_date(self, date: datetime.date) -> str:
        pass
    
    @abstractmethod
    def _create_storage_path_per_date(self, date: datetime.date) -> Path:
        pass
    