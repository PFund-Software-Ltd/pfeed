from __future__ import annotations
from typing import Literal, ClassVar

from pfund.products.product_base import BaseProduct
from pfund.enums import Environment
from pfeed.enums import DataCategory
from pfeed.data_models.time_based_data_model import TimeBasedDataModel, TimeBasedMetadataModel
from pfeed.data_handlers.news_data_handler import NewsDataHandler
from pfeed.data_models.data_provider_model_mixin import DataProviderModelMixin, DataProviderMetadataModelMixin


class NewsMetadataModel(DataProviderMetadataModelMixin, TimeBasedMetadataModel):
    product_name: str | None
    product_basis: str | None
    symbol: str | None
    asset_type: str | None


class NewsDataModel(DataProviderModelMixin, TimeBasedDataModel):
    data_handler_class: ClassVar[type[NewsDataHandler]] = NewsDataHandler
    metadata_class: ClassVar[type[NewsMetadataModel]] = NewsMetadataModel

    env: Environment = Environment.LIVE
    data_category: ClassVar[Literal[DataCategory.NEWS_DATA]] = DataCategory.NEWS_DATA
    product: BaseProduct | None = None  # when product is None, it means general news (e.g. market news)

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