from typing import Literal

from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    # hard-coded "download" instead of MarketFeed.Capability.download to avoid a circular import,
    # MarketFeed.__init_subclass__ checks it matches the capability
    extract_type: Literal["download"] = "download"
