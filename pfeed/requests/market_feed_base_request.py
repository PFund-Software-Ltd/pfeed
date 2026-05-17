from typing import Any

from pydantic import field_validator, Field, model_validator

from pfund.datas.resolution import Resolution
from pfund.entities.products.product_base import BaseProduct
from pfund.enums.env import Environment
from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest


MIN_TARGET_RESOLUTION = Resolution("1d")


class MarketFeedBaseRequest(TimeBasedFeedBaseRequest):
    env: Environment | str
    product: BaseProduct
    data_resolution: Resolution | str = Field(
        description="Resolution of the data extracted from source before being resampled (if any) to target_resolution"
    )
    target_resolution: Resolution | str = Field(description="Final resolution of the output data")

    @field_validator("env", mode="before")
    @classmethod
    def _create_env(cls, v: Environment | str) -> Environment:
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
    def _create_target_resolution(cls, v: Resolution | str) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    @field_validator("data_resolution", mode="before")
    @classmethod
    def _create_data_resolution(cls, v: Resolution | str) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    @model_validator(mode="after")
    def _check_resampleable(self):
        if self.data_resolution and self.data_resolution < self.target_resolution:
            raise ValueError(
                f"data_resolution ({self.data_resolution}) must be >= " +
                f"target_resolution ({self.target_resolution}) for resampling"
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
        }
        if self.data_origin:
            data["data_origin"] = self.data_origin
        if self.storage_config:
            data["storage_config"] = self.storage_config.model_dump()
        return pformat(data, sort_dicts=False)

    def model_post_init(self, __context: Any) -> None:
        from pfund_kit.style import RichColor, TextStyle, cprint
        super().model_post_init(__context)
        if not self.clean_data and self.data_resolution and self.target_resolution < self.data_resolution:
            cprint(
                "Skipping default transformations (e.g. resampling) because clean_data=False/data_layer=RAW, i.e.\n" +
                f"{self.name} {self.product.name} will return {self.data_resolution} data, not requested {self.target_resolution} data.",
                style=TextStyle.BOLD + RichColor.YELLOW
            )
