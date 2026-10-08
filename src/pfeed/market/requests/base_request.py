from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.market.data_model import MarketDataModel

from pfund.datas.resolution import Resolution
from pfund.entities.products.product_base import BaseProduct
from pfund.enums.env import Environment
from pydantic import Field, field_validator, model_validator

from pfeed.base.time_based_request import TimeBasedFeedBaseRequest
from pfeed.enums import DataLayer

MIN_TARGET_RESOLUTION = Resolution("1d")


class MarketFeedBaseRequest(TimeBasedFeedBaseRequest):
    env: Environment
    product: BaseProduct
    target_resolution: Resolution = Field(
        description="Final resolution of the output data"
    )
    data_resolution: Resolution = Field(
        description="Resolution of the data extracted from source before being resampled (if any) to target_resolution",
    )

    @field_validator("target_resolution", mode="after")
    @classmethod
    def _enforce_min_resolution(cls, v: Resolution) -> Resolution:
        if v < MIN_TARGET_RESOLUTION:
            raise ValueError(
                f"target_resolution {v} is below the minimum supported resolution {MIN_TARGET_RESOLUTION}"
            )
        return v

    @model_validator(mode="after")
    def _check_resampleable(self):
        if self.data_resolution < self.target_resolution:
            raise ValueError(
                f"data_resolution ({self.data_resolution}) must be >= "
                + f"target_resolution ({self.target_resolution}) for resampling"
            )
        # raw data is not resampled, e.g. 1minute raw data cannot be produced from a 1tick source/storage
        if self.data_layer == DataLayer.RAW and self.target_resolution < self.data_resolution:
            raise ValueError(
                f"Cannot {self.extract_type} {self.target_resolution} raw data from {self.data_resolution} data"
            )
        return self

    def to_data_model(self) -> MarketDataModel:
        from pfeed import registry
        from pfeed.enums import DataCategory
        from pfeed.market.data_model import MarketDataModel

        # the source's own feed declares the DataModel (plugins may narrow it, e.g. BybitMarketDataModel)
        Feed = registry.get_feed(self.data_source, DataCategory.MARKET_DATA)
        DataModel = Feed.DataModel
        if not issubclass(DataModel, MarketDataModel):
            raise TypeError(
                f"{Feed.__name__}.DataModel must subclass MarketDataModel, got {DataModel}"
            )
        return DataModel(
            env=self.env,
            data_source=self.data_source,
            data_origin=self.data_origin,
            product=self.product,
            resolution=self.target_resolution,
            start_date=self.start_date,
            end_date=self.end_date,
        )

    def __str__(self) -> str:
        from pprint import pformat

        data: dict[str, str] = {
            "data_source": self.data_source,
            "env": str(self.env),
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "product": self.product.name,
            "symbol": self.product.symbol,
            "target_resolution": str(self.target_resolution),
            "data_resolution": str(self.data_resolution),
            "data_layer": str(self.data_layer),
        }
        if self.data_origin != self.data_source:
            data["data_origin"] = self.data_origin
        if self.io:
            data["io"] = repr(self.io)
        return pformat(data, sort_dicts=False)
