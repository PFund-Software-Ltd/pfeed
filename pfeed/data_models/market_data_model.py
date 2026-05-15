from __future__ import annotations
from typing import ClassVar

from pydantic import field_validator, field_serializer

from pfund.enums.env import Environment
from pfund.datas.resolution import Resolution
from pfund.entities.products.product_base import BaseProduct
from pfeed.data_models.time_based_data_model import TimeBasedDataModel
from pfeed.data_handlers.market_data_handler import MarketDataHandler


class MarketDataModel(TimeBasedDataModel):
    data_handler_class: ClassVar[type[MarketDataHandler]] = MarketDataHandler

    env: Environment
    product: BaseProduct
    resolution: Resolution
    
    def __str__(self) -> str:
        return ':'.join([self.env, super().__str__(), str(self.product.asset_type), self.product.symbol, str(self.resolution)])
    
    @field_validator('env', mode='before')
    @classmethod
    def _create_env(cls, v: str | Environment) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v
    
    @field_validator('resolution', mode='before')
    @classmethod
    def _create_resolution(cls, v: str | Resolution) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v
    
    @field_serializer('resolution')
    def _serialize_resolution(self, value: Resolution) -> str:
        return repr(value)
