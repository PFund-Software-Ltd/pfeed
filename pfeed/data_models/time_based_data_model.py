import datetime

from pfeed.data_models.base_data_model import BaseDataModel


class TimeBasedDataModel(BaseDataModel):
    start_date: datetime.date
    end_date: datetime.date | None = None

    @property
    def date(self) -> datetime.date:
        return self.start_date
    
    def update_start_date(self, start_date: datetime.date) -> None:
        self.start_date = start_date
        # update filename and storage path to reflect the new start date
        self.filename = self.create_filename()
        self.storage_path = self.create_storage_path()
        
    def update_end_date(self, end_date: datetime.date | None) -> None:
        self.end_date = end_date
    
    def __str__(self):
        if not self.end_date:
            return ':'.join([super().__str__(), str(self.start_date)])
        else:
            return ':'.join([super().__str__(), '(from)' + str(self.start_date), '(to)' + str(self.end_date)])

    def __hash__(self):
        return hash((self.source.name, self.unique_identifier, self.start_date, self.end_date))
