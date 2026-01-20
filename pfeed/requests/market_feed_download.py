from pydantic import field_validator, Field

from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct
from pfeed.requests.time_based_feed_download import TimeBasedFeedDownloadRequest


class MarketFeedDownloadRequest(TimeBasedFeedDownloadRequest):
    product: BaseProduct
    target_resolution: Resolution = Field(description='The resolution of the data to be stored')
    data_resolution: Resolution = Field(description='The resolution of the downloaded data')
    
    @field_validator('target_resolution', mode='before')
    @classmethod
    def create_target_resolution(cls, v):
        if isinstance(v, str):
            return Resolution(v)
        return v
    
    @field_validator('data_resolution', mode='before')
    @classmethod
    def create_data_resolution(cls, v):
        if isinstance(v, str):
            return Resolution(v)
        return v
