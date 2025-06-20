from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfund.adapter import Adapter
    from pfund.exchanges import Bybit
    from pfund.exchanges.bybit.ws_api import WebsocketApi
    from pfund.products.product_crypto import CryptoProduct

import asyncio
from enum import StrEnum

from pfund.enums import Environment


class BybitStreamingEnvironment(StrEnum):
    PAPER = Environment.PAPER
    LIVE = Environment.LIVE


class StreamAPI:
    def __init__(self, env: Literal['PAPER', 'LIVE']):
        from pfund.exchanges import Bybit
        self._env = BybitStreamingEnvironment[env.upper()]
        exchange = Bybit(env=self._env)
        self._adapter: Adapter = exchange.adapter
        self._ws_api: WebsocketApi = exchange._ws_api
        # TODO: Backpressure if Ray processes fall behind: Use bounded asyncio.Queue(maxsize=N) and await queue.put() to naturally throttle?
        # TODO: should assert self._ws_api._accounts is empty before start
    
    def _get_url(self, ):
        pass
    
    def connect(self):
        pass