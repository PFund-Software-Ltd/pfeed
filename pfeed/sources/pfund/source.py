from typing import ClassVar

from enum import StrEnum

from pfeed.sources.base_source import BaseSource


class PFundDataSource(StrEnum):
    PFund = 'PFund'

    
class PFundSource(BaseSource):
    name: ClassVar[PFundDataSource | StrEnum] = PFundDataSource.PFund
    
    def __init__(self):
        super().__init__(data_categories=[])
