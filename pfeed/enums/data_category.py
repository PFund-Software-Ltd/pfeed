from enum import StrEnum


class DataCategory(StrEnum):
    MARKET_DATA = 'MARKET_DATA'
    NEWS_DATA = 'NEWS_DATA'
    
    @property
    def feed_name(self) -> str:
        # e.g. DataCategory.MARKET_DATA -> market_feed
        return self.lower().replace('_data', '_feed')