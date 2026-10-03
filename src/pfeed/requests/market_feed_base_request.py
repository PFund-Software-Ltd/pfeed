from typing import Any

from pfund.datas.resolution import Resolution
from pfund.entities.products.product_base import BaseProduct
from pfund.enums.env import Environment
from pydantic import Field, field_validator, model_validator

from pfeed.enums import DataLayer
from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest

MIN_TARGET_RESOLUTION = Resolution("1d")


class MarketFeedBaseRequest(TimeBasedFeedBaseRequest):
    env: Environment | str
    product: BaseProduct
    target_resolution: Resolution | str = Field(
        description="Final resolution of the output data"
    )
    data_resolution: Resolution | str | None = Field(
        default=None,
        description="Resolution of the data extracted from source before being resampled (if any) to target_resolution",
    )

    @field_validator("env", mode="before")
    @classmethod
    def _validate_env(cls, v: Environment | str) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("target_resolution", mode="after")
    @classmethod
    def _enforce_min_resolution(cls, v: Resolution) -> Resolution:
        if v < MIN_TARGET_RESOLUTION:
            raise ValueError(
                f"target_resolution {v} is below the minimum supported resolution {MIN_TARGET_RESOLUTION}"
            )
        return v

    @field_validator("target_resolution", mode="before")
    @classmethod
    def _validate_target_resolution(cls, v: Resolution | str) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    @field_validator("data_resolution", mode="before")
    @classmethod
    def _validate_data_resolution(cls, v: Resolution | str) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    @model_validator(mode="after")
    def _check_resampleable(self):
        if self.data_resolution and self.data_resolution < self.target_resolution:
            raise ValueError(
                f"data_resolution ({self.data_resolution}) must be >= "
                + f"target_resolution ({self.target_resolution}) for resampling"
            )
        # raw data is not resampled, e.g. 1minute raw data cannot be produced from a 1tick source/storage
        if (
            self.data_layer == DataLayer.RAW
            and self.data_resolution
            and self.target_resolution < self.data_resolution
        ):
            raise ValueError(
                f"Cannot {self.extract_type} {self.target_resolution} raw data from {self.data_resolution} data"
            )
        return self

    def __str__(self) -> str:
        from pprint import pformat

        data: dict[str, str | bool | dict[str, Any] | None] = {
            "env": str(self.env),
            "start_date": str(self.start_date),
            "end_date": str(self.end_date),
            "product": self.product.name,
            "target_resolution": str(self.target_resolution),
            "data_resolution": str(self.data_resolution),
            "data_layer": str(self.data_layer),
        }
        if self.data_origin:
            data["data_origin"] = self.data_origin
        if self.io:
            data["io"] = repr(self.io)
        return pformat(data, sort_dicts=False)
