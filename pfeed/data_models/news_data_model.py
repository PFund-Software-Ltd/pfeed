from __future__ import annotations
from typing_extensions import TypedDict
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfund.typing import tEnvironment
    from pfeed.enums import DataLayer
    from pfeed.typing import tDataSource

import datetime
from pathlib import Path

from pydantic import model_validator

from pfund.products.product_base import BaseProduct
from pfeed.data_models.time_based_data_model import TimeBasedDataModel
from pfeed.data_handlers import NewsDataHandler


class NewsMetadata(TypedDict, total=True):
    env: tEnvironment
    data_source: tDataSource
    data_origin: str
    start_date: datetime.date
    end_date: datetime.date
    symbol: str | None
    asset_type: str | None
    

class NewsDeltaMetadata(TypedDict, total=True):
    env: tEnvironment
    data_source: tDataSource
    data_origin: str
    dates: list[datetime.date]
    symbol: str | None
    asset_type: str | None


class NewsDataModel(TimeBasedDataModel):
    product: BaseProduct | None = None  # when product is None, it means general news (e.g. market news)

    def __str__(self):
        return ':'.join([super().__str__(), repr(self.product) if self.product else 'GENERAL', 'NEWS'])
    
    @model_validator(mode='before')
    @classmethod
    def check_and_convert(cls, data: dict) -> dict:
        if 'product' in data:
            product = data['product']
            assert isinstance(product, BaseProduct), f'product must be a Product object, got {type(product)}'
        return data
    
    def create_filename(self, date: datetime.date, file_extension='.parquet') -> str:
        name = 'GENERAL_MARKET_NEWS' if self.product is None else self.product.symbol
        filename = '_'.join([name, str(date)])
        return filename + file_extension

    def create_storage_path(self, date: datetime.date, use_deltalake: bool=False) -> Path:
        path = (
            Path(f'env={self.env.value}')
            / f'data_source={self.data_source.name}'
            / f'data_origin={self.data_origin}'
        )
        if use_deltalake:
            return path
        else:
            asset_type = 'NONE' if self.product is None else str(self.product.asset_type)
            symbol = 'NONE' if self.product is None else self.product.symbol
            year, month, day = str(date).split('-')
            return (
                path 
                / f'asset_type={asset_type}'
                / f'symbol={symbol}'   
                / f'year={year}' 
                / f'month={month}' 
                / f'day={day}'
            )
    
    @property
    def data_handler_class(self) -> type[NewsDataHandler]:
        return NewsDataHandler

    def to_metadata(self) -> NewsMetadata:
        return {
            **super().to_metadata(),
            'symbol': self.product.symbol if self.product else None,
            'asset_type': str(self.product.asset_type) if self.product else None,
        }
