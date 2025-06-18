from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.adapter import Adapter
    from pfund.exchanges import Bybit
    from pfund.products.product_crypto import CryptoProduct

from websockets.asyncio.client import connect as ws_connect


class StreamAPI:
    def __init__(self, exchange: Bybit):
        self._exchange = exchange
        self._adapter: Adapter = exchange.adapter
        self._exchange.add_all_product_mappings_to_adapter()
    
    def connect(self):
        pass