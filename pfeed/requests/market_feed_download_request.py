from typing import Literal

from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.download] = ExtractType.download
