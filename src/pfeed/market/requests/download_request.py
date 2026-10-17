from typing import ClassVar

from pydantic import Field

from pfeed.dataflow.retry_policy import RetryPolicy
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    extract_type: ClassVar[str] = "download"
    # a download calls the source's API, so it retries with the source's retry policy by default
    retry_policy: RetryPolicy = Field(default_factory=lambda data: data["data_source"].retry_policy)
