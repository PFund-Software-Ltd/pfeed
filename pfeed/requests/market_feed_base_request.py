from pydantic import field_validator

from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct
from pfund.enums import Environment
from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest


class MarketFeedBaseRequest(TimeBasedFeedBaseRequest):
    env: Environment
    product: BaseProduct
    resolution: Resolution

    @field_validator("env", mode="before")
    @classmethod
    def create_env(cls, v):
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("resolution", mode="before")
    @classmethod
    def create_resolution(cls, v):
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
            "resolution": str(self.resolution),
        }
        if self.data_origin:
            data["data_origin"] = self.data_origin
        return pformat(data, sort_dicts=False)
