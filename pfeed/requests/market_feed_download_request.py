from pydantic import field_validator, Field

from typing import Literal

from pfund.datas.resolution import Resolution
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.download] = ExtractType.download
    data_resolution: Resolution = Field(description="Resolution of the downloaded data")

    @property
    def target_resolution(self) -> Resolution:
        return self.resolution

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
            "target_resolution": str(self.resolution),
            "data_resolution": str(self.data_resolution),
        }
        if self.data_origin:
            data["data_origin"] = self.data_origin
        return pformat(data, sort_dicts=False)
