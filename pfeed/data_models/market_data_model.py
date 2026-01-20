from __future__ import annotations
from typing import ClassVar, Literal

from pydantic import field_validator, field_serializer

from pfund.enums import TradingVenue, Environment
from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct
from pfeed.enums import DataCategory
from pfeed.data_models.time_based_data_model import TimeBasedDataModel, TimeBasedMetadataModel
from pfeed.data_handlers.market_data_handler import MarketDataHandler
from pfeed.data_models.data_provider_model_mixin import DataProviderModelMixin, DataProviderMetadataModelMixin

class MarketMetadataModel(DataProviderMetadataModelMixin, TimeBasedMetadataModel):
    trading_venue: TradingVenue
    exchange: str
    product_name: str
    product_basis: str
    symbol: str
    resolution: str
    asset_type: str
    

class MarketDataModel(DataProviderModelMixin, TimeBasedDataModel):
    data_handler_class: ClassVar[type[MarketDataHandler]] = MarketDataHandler
    metadata_class: ClassVar[type[MarketMetadataModel]] = MarketMetadataModel

    env: Environment
    data_category: ClassVar[Literal[DataCategory.MARKET_DATA]] = DataCategory.MARKET_DATA
    product: BaseProduct
    resolution: Resolution
    
    def __str__(self):
        return ':'.join([self.env, super().__str__(), str(self.product.asset_type), self.product.symbol, repr(self.resolution)])
    
    @field_validator('resolution', mode='before')
    @classmethod
    def create_resolution(cls, v):
        if isinstance(v, str):
            return Resolution(v)
        return v
    
    @field_serializer('resolution')
    def serialize_resolution(self, value: Resolution):
        return repr(value)

    def update_resolution(self, resolution: Resolution | str) -> None:
        if isinstance(resolution, str):
            resolution = Resolution(resolution)
        self.resolution = resolution
    
    def to_metadata(self, **fields) -> MarketMetadataModel:
        return super().to_metadata(
            trading_venue=self.product.trading_venue,
            exchange=self.product.exchange,
            product_name=self.product.name,
            product_basis=str(self.product.basis),
            symbol=self.product.symbol,
            resolution=repr(self.resolution),
            asset_type=str(self.product.asset_type),
            **fields,
        )