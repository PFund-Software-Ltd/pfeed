from enum import StrEnum

from pfeed.sources.base_source import BaseSource


class AlphaFundDataSource(StrEnum):
    AlphaFund = 'AlphaFund'


class AlphaFundDataCategory(StrEnum):
    CHAT_DATA = 'CHAT_DATA'
    
    @property
    def feed_name(self) -> str:
        return self.lower().replace('_data', '_feed')

        
class AlphaFundSource(BaseSource):
    name = AlphaFundDataSource.AlphaFund

    def __init__(self):
        super().__init__(data_categories=list(AlphaFundDataCategory))
