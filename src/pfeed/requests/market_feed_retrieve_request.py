from typing import Any, Literal

from pfund.datas.resolution import Resolution
from pydantic import Field

from pfeed._io.io_config import IOConfig
from pfeed.enums import DataLayer, ExtractType
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.storages.storage_config import StorageConfig


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(
        description="Whether to create a dataflow for each date"
    )
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve
    data_resolution: Resolution | str | None = Field(
        default=None,
        description="Resolution of the data extracted from source before being resampled (if any) to target_resolution",
    )
    storage_config_for_retrieval: StorageConfig = Field(
        description="Storage configuration used for data retrieval, not for loading data to storage"
    )
    io_config_for_retrieval: IOConfig = Field(
        description="IO configuration used for data retrieval, not for loading data to storage"
    )

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        storage_config = self.storage_config_for_retrieval
        if not storage_config:
            raise ValueError("storage config is missing, cannot retrieve data")
        is_raw_data = storage_config.data_layer == DataLayer.RAW
        # if it's not retrieving raw data, there's nothing to clean, clean_data is always False
        if not is_raw_data:
            self.clean_data = False
