from __future__ import annotations
from typing import TYPE_CHECKING, TypeAlias, Callable
if TYPE_CHECKING:
    from collections.abc import Awaitable
    from pfund.entities.products.product_base import BaseProduct
    from pfeed.sources.yahoo_finance.market_data_model import YahooFinanceMarketDataModel
    from pfeed.feeds.streaming_feed_mixin import Message
    

ChannelKey: TypeAlias = str


class StreamAPI:
    def __init__(self, verbose: bool=False):
        import yfinance as yf
        self._ws_api = yf.AsyncWebSocket(verbose=verbose)
        self._callback: Callable[[Message], Awaitable[None] | None] | None = None
        self._channels: list[ChannelKey] = []
        # store the previous 'day_volume' to derive the traded volume, i.e. volume = diff('day_volume' - previous 'day_volume')
        self._last_day_volume: dict[ChannelKey, int] = {}
        # store the last 'time' value to detect duplicated 'time', in milliseconds
        self._last_time_in_mts: dict[ChannelKey, int] = {}  
        
    @property
    def last_day_volume(self) -> dict[ChannelKey, int]:
        return self._last_day_volume
    
    @property
    def last_time_in_mts(self) -> dict[ChannelKey, int]:
        return self._last_time_in_mts
    
    async def connect(self):
        from pfund_kit.style import cprint, TextStyle, RichColor
        cprint(
            "Warning: Yahoo Finance streaming data is not true tick data (every trade) but tick snapshots (aggregated trades)", 
            style=TextStyle.BOLD + RichColor.YELLOW
        )
        await self._ws_api.subscribe(self._channels)
        await self._ws_api.listen(self._callback)  # pyright: ignore[reportUnknownMemberType]
    
    async def disconnect(self):
        # await self._ws_api.unsubscribe(list(self._ws_api._subscriptions))  # no need to unsubscribe, ws already closed
        await self._ws_api.close()
    
    def update_last_day_volume(self, channel_key: ChannelKey, day_volume: int):
        self._last_day_volume[channel_key] = day_volume
        
    def update_last_time_in_mts(self, channel_key: ChannelKey, time_in_mts: int):
        self._last_time_in_mts[channel_key] = time_in_mts
        
    def add_channel(self, data_model: YahooFinanceMarketDataModel) -> ChannelKey:
        assert data_model.resolution.is_tick(), 'Only tick data is supported for Yahoo Finance Streaming, please use .stream(resolution="1tick") instead'
        product: BaseProduct = data_model.product
        if product.is_option() or product.is_crypto() or product.is_future() or product.is_index():
            raise ValueError(f'{product.symbol} is not supported for Yahoo Finance Streaming, only stocks, forex, mutual funds, etfs are supported')
        channel_key: ChannelKey = self.generate_channel_key(product.symbol)
        if channel_key not in self._channels:
            self._channels.append(channel_key)
        return channel_key
    
    @staticmethod
    def generate_channel_key(symbol: str) -> ChannelKey:
        return f'{symbol}'
    
    def set_callback(self, callback: Callable[[Message], Awaitable[None] | None]):
        self._callback = callback
