from __future__ import annotations
from typing import Any, ClassVar, cast

from pydantic import field_validator, field_serializer

from pfund.enums import TradingVenue, Environment
from pfund.datas.resolution import Resolution
from pfund.entities.products.product_base import BaseProduct
from pfeed.data_models.time_based_data_model import TimeBasedDataModel, TimeBasedMetadataModel
from pfeed.data_handlers.market_data_handler import MarketDataHandler

class MarketMetadataModel(TimeBasedMetadataModel):
    trading_venue: TradingVenue
    exchange: str
    product_name: str
    product_basis: str
    symbol: str
    resolution: str
    asset_type: str
    

class MarketDataModel(TimeBasedDataModel):
    data_handler_class: ClassVar[type[MarketDataHandler]] = MarketDataHandler
    metadata_class: ClassVar[type[MarketMetadataModel]] = MarketMetadataModel

    env: Environment
    product: BaseProduct
    resolution: Resolution
    
    def __str__(self) -> str:
        return ':'.join([self.env, super().__str__(), str(self.product.asset_type), self.product.symbol, str(self.resolution)])
    
    @field_validator('env', mode='before')
    @classmethod
    def create_env(cls, v: str | Environment) -> Environment:
        if isinstance(v, str):
            return Environment[v.upper()]
        return v
    
    @field_validator('resolution', mode='before')
    @classmethod
    def create_resolution(cls, v: str | Resolution) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v
    
    @field_serializer('resolution')
    def serialize_resolution(self, value: Resolution) -> str:
        return repr(value)

    def update_resolution(self, resolution: Resolution | str) -> None:
        if isinstance(resolution, str):
            resolution = Resolution(resolution)
        assert isinstance(resolution, Resolution), f'resolution must be a Resolution, but got {type(resolution)}'
        self.resolution = resolution
    
    def to_metadata(self, **fields: Any) -> MarketMetadataModel:
        return cast(MarketMetadataModel, super().to_metadata(
            trading_venue=self.product.trading_venue,
            exchange=self.product.exchange,
            product_name=self.product.name,
            product_basis=str(self.product.basis),
            symbol=self.product.symbol,
            resolution=repr(self.resolution),
            asset_type=str(self.product.asset_type),
            **fields,
        ))
