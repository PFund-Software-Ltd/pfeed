from typing import Literal

from pydantic import Field

from pfeed.enums import ExtractType
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(
        description="Whether to create a dataflow for each date"
    )
    extract_type: Literal[ExtractType.download] = ExtractType.download
