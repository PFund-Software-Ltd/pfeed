from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfeed.market.data_handler import MarketDataHandler
    from pfeed.market.data_model import MarketDataModel

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

    def create_data_resolution_handler(self, data_model: MarketDataModel | None = None) -> MarketDataHandler:
        """Handler reading this request's data from storage at data_resolution.

        The data is stored at data_resolution, e.g. '1t', and resampled to target_resolution, e.g. '1m',
        by the transformations after the read, so the handler reads data_resolution, not target_resolution.

        Args:
            data_model: date range to read, e.g. one date of a per-date dataflow.
                Defaults to the request's full range.
        """
        from pfeed.enums import DataCategory

        data_model = (data_model or self.to_data_model()).model_copy(update={"resolution": self.data_resolution})
        return data_model.DataHandler(
            data_model=data_model,
            io=self.io_for_retrieval,
            data_layer=self.data_layer,
            data_domain=str(DataCategory.MARKET_DATA),
        )
