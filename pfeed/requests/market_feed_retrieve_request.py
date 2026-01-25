from typing import Literal

from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve
