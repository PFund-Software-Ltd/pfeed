from typing import Any

from pydantic import BaseModel, ConfigDict

from pfeed.const.enums.env import Environment
from pfeed.sources.base_data_source import BaseDataSource


class BaseDataModel(BaseModel):
    '''
    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        source: The source of the data.
        unique_identifier:
            A unique identifier for the data.
            If specified, it will be used to differentiate where the data is actually from.
                For example, 
                    for Databento, Publisher (comprised of dataset and trading venue, e.g. DATASET_VENUE) is used to be the unique identifier.
                    This is because a product can be traded on multiple venues.
            If None, it means the data source is already a unique identifier.
            Default is None.
    '''
    model_config = ConfigDict(arbitrary_types_allowed=True)

    env: Environment
    source: BaseDataSource
    file_extension: str = ''
    unique_identifier: str = ''

    def model_post_init(self, __context: Any) -> None:
        if not self.unique_identifier:
            self.unique_identifier = self.source.name.value

    def __str__(self):
        if self.unique_identifier:
            return f'{self.source.name.value}_{self.unique_identifier}'
        else:
            return f'{self.source.name.value}'
