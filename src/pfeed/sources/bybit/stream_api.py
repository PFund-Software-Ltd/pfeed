from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from pfund.datas.resolution import Resolution
    from pfund.entities.products.product_bybit import BybitProduct
    from pfund.typing import FullDataChannel

    from pfeed.feeds.streaming_feed_mixin import RawMessage, WebSocketName
    from pfeed.sources.bybit.market_data_model import BybitMarketDataModel

    BybitProductCategory: TypeAlias = BybitProduct.Category
    ChannelKey: TypeAlias = tuple[BybitProductCategory, FullDataChannel]

from pfund.enums.env import Environment


class StreamAPI:
    """Simple wrapper of exchange's websocket API to connect to public channels"""

    def __init__(self, env: Environment | str):
        from pfund.brokers.crypto.exchanges.bybit.ws_api import WebSocketAPI

        self._env = Environment[env.upper()]
        self._ws_api = WebSocketAPI(self._env)
        # set the logger to be "bybit_stream", override the default logger 'bybit' in pfund
        self._ws_api.set_logger(f"{self._ws_api.exch.lower()}_data")

    @property
    def env(self) -> Environment:
        return self._env

    async def connect(self):
        assert not self._ws_api._accounts, "Accounts should be empty in streaming"
        await self._ws_api.connect()

    async def disconnect(self):
        await self._ws_api.disconnect()

    def add_channel(
        self,
        data_model: BybitMarketDataModel,
        data_resolution: Resolution | None = None,
    ) -> ChannelKey:
        product: BybitProduct = data_model.product
        resolution: Resolution = (
            data_resolution or data_model.resolution
        )  # data model is using target resolution
        category = product.category
        assert category is not None, "product.category is not initialized"
        channel: FullDataChannel = self._ws_api._create_public_channel(
            product, resolution
        )
        self._ws_api.add_channel(channel, channel_type="public", category=category)
        channel_key: ChannelKey = self.generate_channel_key(category, channel)
        return channel_key

    @staticmethod
    def generate_channel_key(
        category: BybitProductCategory, channel: FullDataChannel
    ) -> ChannelKey:
        return (category, channel)

    def set_callback(
        self,
        faucet_callback: Callable[
            [WebSocketName, RawMessage, ChannelKey | None], Coroutine[Any, Any, None]
        ],
    ):
        from pfund.entities.products.product_bybit import BybitProduct

        async def _callback(ws_name: WebSocketName, msg: RawMessage):
            if "topic" in msg:
                channel: str = msg["topic"]
                category = ws_name.split("_")[1]
                category = BybitProduct.Category[category.upper()]
                channel_key: ChannelKey = self.generate_channel_key(category, channel)
                # NOTE: Bybit's tick (publicTrade) messages and bar (kline) messages when it's closed contain multiple trades in a single message,
                # split them into individual messages so each tick/bar flows through the pipeline separately
                if isinstance(msg.get("data"), list):
                    for item in msg["data"]:
                        individual_msg = {**msg, "data": item}
                        await faucet_callback(ws_name, individual_msg, channel_key)
                    return
            else:
                channel_key: tuple[str, str] | None = None
            await faucet_callback(ws_name, msg, channel_key)

        self._ws_api.set_callback(_callback, raw_msg=True)
