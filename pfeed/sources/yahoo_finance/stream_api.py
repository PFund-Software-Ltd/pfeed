from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Callable, Awaitable
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from pfeed.sources.yahoo_finance.market_data_model import YahooFinanceMarketDataModel
    

ChannelKey: TypeAlias = str


class StreamAPI:
    def __init__(self, verbose=False):
        import yfinance as yf
        self._ws_api = yf.AsyncWebSocket(verbose=verbose)
        self._streaming_bindings: dict[ChannelKey, YahooFinanceMarketDataModel] = {}
        self._callback: Callable[[dict], Awaitable[None] | None] | None = None
        # store the previous 'day_volume' to derive the traded volume, i.e. volume = diff('day_volume' - previous 'day_volume')
        self._last_day_volume: dict[ChannelKey, int] = {}
        # store the last 'time' value to detect duplicated 'time', in milliseconds
        self._last_time_in_mts: dict[ChannelKey, int] = {}  
    
    async def connect(self):
        from pfund_kit.style import cprint, TextStyle, RichColor
        cprint("Warning: Yahoo Finance streaming data is not true tick data (every trade) but tick snapshots (aggregated trades)", style=TextStyle.BOLD + RichColor.YELLOW)
        symbols = [data_model.product.symbol for data_model in self._streaming_bindings.values()]
        await self._ws_api.subscribe(symbols)
        await self._ws_api.listen(self._callback)
    
    async def disconnect(self):
        # await self._ws_api.unsubscribe(list(self._ws_api._subscriptions))  # no need to unsubscribe, ws already closed
        await self._ws_api.close()
    
    def _add_data_channel(self, data_model: YahooFinanceMarketDataModel):
        assert data_model.resolution.is_tick(), 'Only tick data is supported for Yahoo Finance Streaming, please use .stream(resolution="1tick") instead'
        product: BaseProduct = data_model.product
        if product.is_option() or product.is_crypto() or product.is_future() or product.is_index():
            raise ValueError(f'{product.symbol} is not supported for Yahoo Finance Streaming, only stocks, forex, mutual funds, etfs are supported')
        channel_key: ChannelKey = self.generate_channel_key(product.symbol)
        self._streaming_bindings[channel_key] = data_model
    
    @staticmethod
    def generate_channel_key(symbol: str) -> ChannelKey:
        return f'{symbol}'
    
    def set_callback(self, callback: Callable[[dict], Awaitable[None] | None]):
        self._callback = callback
    