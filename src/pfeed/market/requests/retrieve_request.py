from typing import Literal

from pfund.datas.resolution import Resolution
from pydantic import Field

from pfeed.io.base_io import BaseIO
from pfeed.market.feed import MarketFeed
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(
        description="Whether to create a dataflow for each date"
    )
    extract_type: Literal[MarketFeed.Capability.retrieve] = (
        MarketFeed.Capability.retrieve
    )
    data_resolution: Resolution | str | None = Field(
        default=None,
        description="Resolution of the data extracted from source before being resampled (if any) to target_resolution",
    )
    io_for_retrieval: BaseIO = Field(
        description="IO used for data retrieval, not for loading data to storage"
    )

    def should_clean_data(self) -> bool:
        # retrieved data is returned as stored, raw data is never cleaned on read
        return False
