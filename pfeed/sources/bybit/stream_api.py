from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, Awaitable
if TYPE_CHECKING:
    from pfund.products.product_bybit import BybitProduct
    from pfund.exchanges.bybit.exchange import tProductCategory
    from pfund.datas.resolution import Resolution
    from pfund._typing import FullDataChannel
    from pfund.exchanges.bybit.ws_api import WebsocketApi


class StreamAPI:
    '''Simple wrapper of exchange's websocket API to connect to public channels'''
    def __init__(self, ws_api: WebsocketApi):
        self._ws_api: WebsocketApi = ws_api
        # set the logger to be "bybit_stream", override the default logger 'bybit' in pfund
        self._ws_api.set_logger(f'{self._ws_api.exch.lower()}_data')
    
    async def connect(self):
        assert not self._ws_api._accounts, 'Accounts should be empty in streaming'
        await self._ws_api.connect()
    
    async def disconnect(self):
        await self._ws_api.disconnect()

    def _add_data_channel(self, product: BybitProduct, resolution: Resolution):
        channel: FullDataChannel = self._ws_api._create_public_channel(product, resolution)
        self.add_channel(channel, channel_type='public', category=product.category)
        return channel
    
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
    