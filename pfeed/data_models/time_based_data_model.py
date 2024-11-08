import datetime

from pfeed.data_models.base_data_model import BaseDataModel


class TimeBasedDataModel(BaseDataModel):
    date: datetime.date

    def __str__(self):
        return '_'.join([super().__str__(), str(self.date)])
