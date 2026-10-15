from typing import ClassVar

from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    extract_type: ClassVar[str] = "download"
