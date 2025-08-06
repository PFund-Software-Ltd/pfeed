from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, Awaitable
if TYPE_CHECKING:
    from pfund.exchanges.bybit.exchange import tProductCategory
    from pfund.datas.resolution import Resolution
    from pfund.exchanges.bybit.ws_api import WebSocketAPI
    from pfeed.sources.bybit.market_data_model import BybitMarketDataModel

from pfund._typing import FullDataChannel
from pfund.products.product_bybit import BybitProduct


ChannelKey = tuple[BybitProduct.ProductCategory, FullDataChannel]


class StreamAPI:
    '''Simple wrapper of exchange's websocket API to connect to public channels'''
    def __init__(self, ws_api: WebSocketAPI):
        self._ws_api: WebSocketAPI = ws_api
        # set the logger to be "bybit_stream", override the default logger 'bybit' in pfund
        self._ws_api.set_logger(f'{self._ws_api.exch.lower()}_data')
        self._streaming_bindings: dict[ChannelKey, BybitMarketDataModel] = {}
    
    async def connect(self):
        assert not self._ws_api._accounts, 'Accounts should be empty in streaming'
        await self._ws_api.connect()
    
    async def disconnect(self):
        await self._ws_api.disconnect()

    def _add_data_channel(self, data_model: BybitMarketDataModel) -> FullDataChannel:
        product: BybitProduct = data_model.product
        resolution: Resolution = data_model.resolution
        channel: FullDataChannel = self._ws_api._create_public_channel(product, resolution)
        self.add_channel(channel, channel_type='public', category=product.category)
        channel_key: ChannelKey = self._generate_channel_key(product.category, channel)
        self._bind_channel_key_to_data_model(channel_key, data_model)
        return channel
    
    @staticmethod
    def _generate_channel_key(category: BybitProduct.ProductCategory, channel: FullDataChannel) -> ChannelKey:
        return (category, channel)
    
    def _bind_channel_key_to_data_model(
        self, 
        channel_key: ChannelKey,
        data_model: BybitMarketDataModel, 
    ):
        self._streaming_bindings[channel_key] = data_model
    
    def add_channel(
        self, 
        channel: FullDataChannel, 
        *,
        channel_type: Literal['public', 'private'], 
        category: tProductCategory | None = None,
    ):
        self._ws_api.add_channel(channel, channel_type=channel_type, category=category)
    
    def set_callback(self, callback: Callable[[dict], Awaitable[None] | None]):
        self._ws_api.set_callback(callback, raw_msg=True)
    