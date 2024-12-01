from typing import Any
from abc import ABC, abstractmethod
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from pfeed.const.enums.env import Environment
from pfeed.sources.base_data_source import BaseDataSource


class BaseDataModel(BaseModel, ABC):
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
    filename: str = ''
    filename_prefix: str = ''
    filename_suffix: str = ''
    file_extension: str = ''
    storage_path: Path | None = None
    unique_identifier: str = ''
    compression: str = ''
    metadata: dict | None = None

    def model_post_init(self, __context: Any) -> None:
        if not self.unique_identifier:
            self.unique_identifier = self.source.name.value
        self.filename = self.create_filename()
        self.storage_path = self.create_storage_path()

    def __str__(self):
        if self.unique_identifier:
            return f'{self.source.name.value}_{self.unique_identifier}'
        else:
            return f'{self.source.name.value}'

    def __hash__(self):
        return hash((self.source.name, self.unique_identifier))

    @abstractmethod
    def create_filename(self) -> str:
        pass
    
    @abstractmethod
    def create_storage_path(self) -> Path:
        pass