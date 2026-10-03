from typing import Literal

from pfund.datas.resolution import Resolution
from pydantic import Field

from pfeed.enums import ExtractType
from pfeed.io.base_io import BaseIO
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(
        description="Whether to create a dataflow for each date"
    )
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve
    data_resolution: Resolution | str | None = Field(
        default=None,
        description="Resolution of the data extracted from source before being resampled (if any) to target_resolution",
    )
    io_for_retrieval: BaseIO = Field(
        description="IO used for data retrieval, not for loading data to storage"
    )

    @property
    def should_clean_data(self) -> bool:
        # retrieved data is returned as stored, raw data is never cleaned on read
        return False
