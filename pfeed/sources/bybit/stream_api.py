from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.enums import Environment
    from pfund.adapter import Adapter
    from pfund.exchanges.bybit.ws_api import WebsocketApi

import asyncio


class StreamAPI:
    def __init__(self, env: Environment):
        from pfund.exchanges import Bybit
        exchange = Bybit(env=env)
        self._adapter: Adapter = exchange.adapter
        self._ws_api: WebsocketApi = exchange._ws_api
        # TODO: Backpressure if Ray processes fall behind: Use bounded asyncio.Queue(maxsize=N) and await queue.put() to naturally throttle?
        # TODO: should assert self._ws_api._accounts is empty before start
    
    def _get_url(self, ):
        pass
    
    def connect(self):
        pass