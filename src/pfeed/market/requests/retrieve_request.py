from typing import ClassVar

from pydantic import Field

from pfeed.io.base_io import BaseIO
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    extract_type: ClassVar[str] = "retrieve"
    io_for_retrieval: BaseIO = Field(
        description="IO used for data retrieval, not for loading data to storage"
    )

    def should_clean_data(self) -> bool:
        # retrieved data is returned as stored, raw data is never cleaned on read
        return False
