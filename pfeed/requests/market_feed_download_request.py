from typing import Literal, Any

from pydantic import Field, field_validator

from pfund.datas.resolution import Resolution
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(description='Whether to create a dataflow for each date')
    data_resolution: Resolution | str = Field(
        description="Resolution of the raw data to download from source before resampling (if any) to target_resolution"
    )
    extract_type: Literal[ExtractType.download] = ExtractType.download
    
    @field_validator("data_resolution", mode="before")
    @classmethod
    def create_data_resolution(cls, v: Resolution | str) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.storage_config and self.storage_config.data_domain and self.storage_config.data_layer != DataLayer.CURATED:
            raise ValueError(f'Custom data_domain={self.storage_config.data_domain} is only allowed when data layer is CURATED, but got data_layer={self.storage_config.data_layer}')
        if self.storage_config and self.storage_config.data_layer == DataLayer.RAW and self.target_resolution != self.data_resolution:
            raise ValueError(f'No raw data for {self.product.name} with such resolution {self.target_resolution}')
