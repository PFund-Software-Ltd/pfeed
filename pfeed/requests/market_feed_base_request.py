from pydantic import field_validator, Field

from pfund.datas.resolution import Resolution
from pfund.entities.products.product_base import BaseProduct
from pfund.enums import Environment
from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest


class MarketFeedBaseRequest(TimeBasedFeedBaseRequest):
    env: Environment | str
    product: BaseProduct
    target_resolution: Resolution | str = Field(description="Final resolution of the output data")

    @field_validator("env", mode="before")
    @classmethod
    def create_env(cls, v: Environment | str) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("target_resolution", mode="before")
    @classmethod
    def create_target_resolution(cls, v: Resolution | str) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v
    
    def __str__(self) -> str:
        from pprint import pformat

        data: dict[str, str | bool] = {
            "env": str(self.env),
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "product": self.product.name,
            "resolution": str(self.target_resolution),
        }
        if self.data_origin:
            data["data_origin"] = self.data_origin
        return pformat(data, sort_dicts=False)
