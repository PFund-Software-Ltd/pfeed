from __future__ import annotations
from typing import ClassVar, Literal

from pydantic import model_validator

from pfund.enums import TradingVenue, Environment
from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct
from pfeed.enums import DataCategory
from pfeed.data_models.time_based_data_model import TimeBasedDataModel, TimeBasedMetadataModel
from pfeed.data_handlers.market_data_handler import MarketDataHandler


class MarketMetadataModel(TimeBasedMetadataModel):
    env: Environment
    trading_venue: TradingVenue
    exchange: str
    product_name: str
    product_basis: str
    product_specs: dict
    symbol: str
    resolution: Resolution
    asset_type: str
    

class MarketDataModel(TimeBasedDataModel):

    data_handler_class: ClassVar[type[MarketDataHandler]] = MarketDataHandler
    metadata_class: ClassVar[type[MarketMetadataModel]] = MarketMetadataModel

    env: Environment
    data_category: ClassVar[Literal[DataCategory.MARKET_DATA]] = DataCategory.MARKET_DATA
    product: BaseProduct
    resolution: Resolution
    
    def __str__(self):
        return ':'.join([self.env, super().__str__(), str(self.product.asset_type), self.product.symbol, repr(self.resolution)])
    
    # FIXME: this is not needed anymore?
    @model_validator(mode='before')
    @classmethod
    def check_and_convert(cls, data: dict) -> dict:
        product = data['product']
        assert isinstance(product, BaseProduct), f'product must be a Product object, got {type(product)}'
        # convert resolution to Resolution object if it is a string
        resolution = data['resolution']
        if isinstance(resolution, str):
            resolution = Resolution(resolution)
        data['resolution'] = resolution
        return data
    
    def to_metadata(self) -> MarketMetadataModel:
        return MarketMetadataModel(
            **super().to_metadata().model_dump(),
            env=self.env,
            trading_venue=self.product.trading_venue,
            exchange=self.product.exchange,
            product_name=self.product.name,
            product_basis=str(self.product.basis),
            product_specs=self.product.specs,
            symbol=self.product.symbol,
            resolution=self.resolution,
            asset_type=str(self.product.asset_type),
        )