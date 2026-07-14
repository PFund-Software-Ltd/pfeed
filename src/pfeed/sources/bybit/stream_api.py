from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from pfund.typing import FullDataChannel
    from pfund.venues.bybit.product import BybitProduct

    from pfeed.feeds.streaming_feed_mixin import RawMessage, WebSocketName
    from pfeed.sources.bybit.market_data_model import BybitMarketDataModel

    BybitProductCategory: TypeAlias = BybitProduct.Category
    ChannelKey: TypeAlias = tuple[BybitProductCategory, FullDataChannel]

import logging

from pfund.datas.resolution import Resolution
from pfund.enums.env import Environment


class StreamAPI:
    """Simple wrapper of exchange's websocket API to connect to public channels"""

    def __init__(self, env: Literal[Environment.PAPER, Environment.LIVE]):
        from pfund.venues.bybit.ws_api import BybitWebSocketAPI

        self._ws_api = BybitWebSocketAPI(env=env, read_only=True)
        logger = logging.getLogger("pfeed.bybit")
        self._ws_api._logger = logger
        for api in self._ws_api._apis.values():
            api._logger = logger

    @property
    def env(self) -> Environment:
        return self._ws_api.env

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
        resolution: Resolution = data_resolution or Resolution(
            data_model.resolution
        )  # data model is using target resolution
        category = product.category
        assert category is not None, "product.category is not initialized"
        channel: FullDataChannel = self._ws_api._create_market_data_channel(
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
        from pfund.venues.bybit.product import BybitProduct

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

        self._ws_api.set_callback(_callback, raw_msg=True)  # pyright: ignore[reportArgumentType]
