from typing import ClassVar

from enum import StrEnum

from pfeed.sources.base_source import BaseSource


class PFundDataSource(StrEnum):
    PFund = 'PFund'

    
class PFundDataCategory(StrEnum):
    ENGINE_DATA = 'ENGINE_DATA'
    COMPONENT_DATA = 'COMPONENT_DATA'

    @property
    def feed_name(self) -> str:
        return self.lower().replace('_data', '_feed')


class PFundSource(BaseSource):
    name: ClassVar[PFundDataSource | StrEnum] = PFundDataSource.PFund
    
    def __init__(self):
        super().__init__(data_categories=list(PFundDataCategory))
