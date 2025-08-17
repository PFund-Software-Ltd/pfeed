from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Callable, Awaitable
if TYPE_CHECKING:
    from pfund._typing import ResolutionRepr
    from pfund.products.product_base import BaseProduct
    from pfeed.sources.yahoo_finance.market_data_model import YahooFinanceMarketDataModel
    

ChannelKey: TypeAlias = str


class StreamAPI:
    def __init__(self, verbose=True):
        import yfinance as yf
        self._ws_api = yf.AsyncWebSocket(verbose=verbose)
        self._streaming_bindings: dict[ChannelKey, YahooFinanceMarketDataModel] = {}
        self._callback: Callable[[dict], Awaitable[None] | None] | None = None
    
    async def connect(self):
        await self._ws_api.subscribe()
        await self._ws_api.listen(self._callback)
    
    async def disconnect(self):
        await self._ws_api.unsubscribe(self._ws_api._subscriptions)
        await self._ws_api.close()
    
    def _add_data_channel(self, data_model: YahooFinanceMarketDataModel):
        product: BaseProduct = data_model.product
        channel_key: ChannelKey = self.generate_channel_key(product.symbol, repr(data_model.resolution))
        self._streaming_bindings[channel_key] = data_model
    
    @staticmethod
    def generate_channel_key(symbol: str, resolution: ResolutionRepr) -> ChannelKey:
        return f'{symbol}.{resolution}'
        
    
    def set_callback(self, callback: Callable[[dict], Awaitable[None] | None]):
        self._callback = callback
    