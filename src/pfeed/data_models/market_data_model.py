from __future__ import annotations

from typing import ClassVar

from pfund.datas.resolution import Resolution
from pfund.entities.products.product_base import BaseProduct
from pfund.enums.env import Environment
from pydantic import field_serializer, field_validator

from pfeed.data_handlers.market_data_handler import MarketDataHandler
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


class MarketDataModel(TimeBasedDataModel):
    DataHandler: ClassVar[type[MarketDataHandler]] = MarketDataHandler

    env: Environment | str
    product: BaseProduct
    resolution: Resolution | str

    def __str__(self) -> str:
        return ":".join(
            [
                self.env,
                super().__str__(),
                str(self.product.asset_type),
                self.product.symbol,
                str(self.resolution),
            ]
        )

    @field_validator("env", mode="before")
    @classmethod
    def _validate_env(cls, v: str | Environment) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v

    @field_validator("resolution", mode="before")
    @classmethod
    def _validate_resolution(cls, v: str | Resolution) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    @field_serializer("resolution")
    def _serialize_resolution(self, value: Resolution) -> str:
        return repr(value)
