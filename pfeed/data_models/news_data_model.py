from __future__ import annotations
from typing import ClassVar

from pydantic import field_validator

from pfund.entities.products.product_base import BaseProduct
from pfund.enums import Environment
from pfeed.data_models.time_based_data_model import TimeBasedDataModel, TimeBasedMetadataModel
from pfeed.data_handlers.news_data_handler import NewsDataHandler


class NewsMetadataModel(TimeBasedMetadataModel):
    product_name: str | None
    product_basis: str | None
    symbol: str | None
    asset_type: str | None


class NewsDataModel(TimeBasedDataModel):
    data_handler_class: ClassVar[type[NewsDataHandler]] = NewsDataHandler
    metadata_class: ClassVar[type[NewsMetadataModel]] = NewsMetadataModel

    env: Environment
    product: BaseProduct | None = None  # when product is None, it means general news (e.g. market news)

    @field_validator('env', mode='before')
    @classmethod
    def create_env(cls, v):
        if isinstance(v, str):
            return Environment[v.upper()]
        return v
    
    def __str__(self):
        return ':'.join([self.env, super().__str__(), repr(self.product) if self.product else 'GENERAL', 'NEWS'])
    
    def to_metadata(self, **fields) -> NewsMetadataModel:
        return super().to_metadata(
            product_name=self.product.name if self.product else None,
            product_basis=str(self.product.basis) if self.product else None,
            symbol=self.product.symbol if self.product else None,
            asset_type=str(self.product.asset_type) if self.product else None,
            **fields,
        )