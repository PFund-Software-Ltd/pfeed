from __future__ import annotations
from typing import TYPE_CHECKING, Callable, TypeAlias
if TYPE_CHECKING:
    from collections.abc import Awaitable
    from pfund.datas.resolution import Resolution
    from pfund.brokers.crypto.exchanges.bybit.exchange import ProductCategory
    from pfeed.sources.bybit.market_data_model import BybitMarketDataModel
    from pfeed.feeds.streaming_feed_mixin import WebSocketName, Message

from pfund.enums import Environment
from pfund.typing import FullDataChannel
from pfund.brokers.crypto.exchanges.bybit.ws_api import WebSocketAPI
from pfund.entities.products.product_bybit import BybitProduct


ChannelKey: TypeAlias = tuple['ProductCategory', FullDataChannel]


class StreamAPI:
    '''Simple wrapper of exchange's websocket API to connect to public channels'''
    def __init__(self, env: Environment | str):
        self._env = Environment[env.upper()]
        self._ws_api = WebSocketAPI(self._env)
        # set the logger to be "bybit_stream", override the default logger 'bybit' in pfund
        self._ws_api.set_logger(f'{self._ws_api.exch.lower()}_data')
    
    @property
    def env(self) -> Environment:
        return self._env
    
    async def connect(self):
        assert not self._ws_api._accounts, 'Accounts should be empty in streaming'
        await self._ws_api.connect()
    
    async def disconnect(self):
        await self._ws_api.disconnect()
    
    def add_channel(self, data_model: BybitMarketDataModel) -> ChannelKey:
        product: BybitProduct = data_model.product
        resolution: Resolution = data_model.resolution
        category = product.category
        assert category is not None, 'product.category is not initialized'
        channel: FullDataChannel = self._ws_api._create_public_channel(product, resolution)
        self._ws_api.add_channel(channel, channel_type='public', category=category)
        channel_key: ChannelKey = self.generate_channel_key(category, channel)
        return channel_key
        
    @staticmethod
    def generate_channel_key(category: ProductCategory, channel: FullDataChannel) -> ChannelKey:
        return (category, channel)
    
    def set_callback(self, callback: Callable[[WebSocketName, Message], Awaitable[None] | None]):
        self._ws_api.set_callback(callback, raw_msg=True)
