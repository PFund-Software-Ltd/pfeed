from typing import Literal

from pydantic import Field

from pfeed.io.base_io import BaseIO
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    # hard-coded "retrieve" instead of MarketFeed.Capability.retrieve to avoid a circular import,
    # MarketFeed.__init_subclass__ checks it matches the capability
    extract_type: Literal["retrieve"] = "retrieve"
    io_for_retrieval: BaseIO = Field(
        description="IO used for data retrieval, not for loading data to storage"
    )

    def should_clean_data(self) -> bool:
        # retrieved data is returned as stored, raw data is never cleaned on read
        return False
