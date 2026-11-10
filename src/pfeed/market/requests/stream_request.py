from pydantic import Field

from pfeed.base.requests.stream_request import BaseStreamRequest
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedStreamRequest(BaseStreamRequest, MarketFeedBaseRequest):
    store_incremental_bars: bool = Field(
        default=False,
        description="Whether to also store the updates of a bar before it closes. Only used when storing streamed bars.",
    )
