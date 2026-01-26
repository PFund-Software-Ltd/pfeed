from pydantic import field_validator, Field

from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct
from pfund.enums import Environment
from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest


class MarketFeedBaseRequest(TimeBasedFeedBaseRequest):
    env: Environment
    product: BaseProduct
    target_resolution: Resolution = Field(description="Final resolution of the output data after resampling (if any)")
    data_resolution: Resolution = Field(description="Resolution of the raw data to extract from source before resampling to target_resolution")

    @field_validator("env", mode="before")
    @classmethod
    def create_env(cls, v):
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("target_resolution", mode="before")
    @classmethod
    def create_target_resolution(cls, v):
        if isinstance(v, str):
            return Resolution(v)
        return v
    
    @field_validator("data_resolution", mode="before")
    @classmethod
    def create_data_resolution(cls, v):
        if isinstance(v, str):
            return Resolution(v)
        return v

    def __str__(self):
        from pprint import pformat

        data = {
            "env": self.env.value,
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "product": self.product.name,
            "target_resolution": str(self.target_resolution),
            "data_resolution": str(self.data_resolution),
        }
        if self.data_origin:
            data["data_origin"] = self.data_origin
        return pformat(data, sort_dicts=False)
