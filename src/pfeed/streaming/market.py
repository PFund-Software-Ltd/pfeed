from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Hashable

    from pfund_blocks.typing import Message, ProductName

from pfeed.market.data_model import MarketDataModel
from pfeed.market.requests import MarketFeedStreamRequest
from pfeed.streaming.mixin import StreamingMixin


class MarketFeedStreamingMixin[RequestT: MarketFeedStreamRequest, DataModelT: MarketDataModel](StreamingMixin):
    @staticmethod
    def _create_queue_key_from_request(request: RequestT) -> tuple[ProductName, str]:
        return (request.product.name, repr(request.data_resolution))

    @staticmethod
    def _create_queue_key_from_message(message: Message) -> Hashable:
        return (message["product"], message["resolution"])
