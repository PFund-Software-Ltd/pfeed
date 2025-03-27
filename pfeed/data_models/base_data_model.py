from typing import Any
from abc import ABC, abstractmethod
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from pfeed.enums.env import Environment
from pfeed.sources.base_source import BaseSource
from pfeed.data_handlers.base_data_handler import BaseDataHandler


class BaseDataModel(BaseModel, ABC):
    '''
    Args:
        env: trading environment, e.g. 'PAPER' | 'LIVE'.
        source: The source of the data.
        data_origin:
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
    data_source: BaseSource
    file_extension: str = ''
    compression: str = ''
    data_origin: str = ''
    # metadata: dict = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        if not self.data_origin:
            self.data_origin = self.data_source.name.value
    
    def is_data_origin_effective(self) -> bool:
        '''
        A data_origin is not effective if it is the same as the source name.
        '''
        return self.data_origin != self.data_source.name.value
    
    # def set_metadata(self, metadata: dict) -> None:
    #     assert not self.metadata, 'metadata is already set, use update_metadata() instead'
    #     self.metadata = metadata

    # def add_metadata(self, key: str, value: Any) -> None:
    #     self.metadata[key] = value

    def __str__(self):
        if self.is_data_origin_effective():
            return f'{self.data_source.name.value}:{self.data_origin}'
        else:
            return f'{self.data_source.name.value}'

    @abstractmethod
    def create_filename(self, *args, **kwargs) -> str:
        pass
    
    @abstractmethod
    def create_storage_path(self, *args, **kwargs) -> Path:
        pass

    @abstractmethod
    def create_data_handler(self, *args, **kwargs) -> BaseDataHandler:
        pass