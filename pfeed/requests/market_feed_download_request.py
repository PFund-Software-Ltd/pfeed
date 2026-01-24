from pydantic import field_validator, Field

from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct
from pfund.enums import Environment
from pfeed.requests.time_based_feed_download_request import TimeBasedFeedDownloadRequest


class MarketFeedDownloadRequest(TimeBasedFeedDownloadRequest):
    env: Environment
    product: BaseProduct
    target_resolution: Resolution = Field(description='The resolution of the data to be stored')
    data_resolution: Resolution = Field(description='The resolution of the downloaded data')
    
    @field_validator('env', mode='before')
    @classmethod
    def create_env(cls, v):
        if isinstance(v, str):
            return Environment[v.upper()]
        return v
    
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
    
    def __str__(self):
        from pprint import pformat
        data = {
            'env': self.env.value,
            'start_date': str(self.start_date),
            'end_date': str(self.end_date),
            'product': self.product.name,
            'resolution': self.target_resolution,
        }
        if self.data_origin:
            data['data_origin'] = self.data_origin
        return pformat(data, sort_dicts=False)
    