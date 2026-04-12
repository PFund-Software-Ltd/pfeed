# pyright: reportGeneralTypeIssues=false
from pfund.datas.resolution import Resolution
from pfeed.streaming.streaming_message import StreamingMessage
from pfeed.enums import DataCategory


class MarketDataMessage(StreamingMessage, frozen=True):
    data_category: DataCategory = DataCategory.MARKET_DATA
    product: str  # product.name
    basis: str
    symbol: str
    resolution: str

    def is_bar(self) -> bool:
        return Resolution(self.resolution).is_bar()
    
    def is_tick(self) -> bool:
        return Resolution(self.resolution).is_tick()
    
    def is_quote(self) -> bool:
        return Resolution(self.resolution).is_quote()
