from pfeed.streaming.streaming_message import StreamingMessage
from pfeed.enums import DataCategory


class MarketDataMessage(StreamingMessage, kw_only=True, frozen=True):
    data_category: DataCategory = DataCategory.MARKET_DATA
    product: str  # product.name
    basis: str
    symbol: str
    resolution: str

    def is_bar(self) -> bool:
        return False
    
    def is_tick(self) -> bool:
        return False
    
    def is_quote(self) -> bool:
        return False
