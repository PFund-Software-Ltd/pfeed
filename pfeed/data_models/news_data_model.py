from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.enums import DataLayer

import datetime
from pathlib import Path

from pydantic import model_validator

from pfund.products.product_base import BaseProduct
from pfeed.data_models.time_based_data_model import TimeBasedDataModel
from pfeed.data_handlers import NewsDataHandler


class NewsDataModel(TimeBasedDataModel):
    product: BaseProduct | None = None  # when product is None, it means general news (e.g. market news)
    file_extension: str = '.parquet'
    compression: str = 'snappy'

    def __str__(self):
        return ':'.join([super().__str__(), repr(self.product) if self.product else 'GENERAL', 'NEWS'])
    
    @model_validator(mode='before')
    @classmethod
    def check_and_convert(cls, data: dict) -> dict:
        if 'product' in data:
            product = data['product']
            assert isinstance(product, BaseProduct), f'product must be a Product object, got {type(product)}'
        return data
    
    def create_filename(self, date: datetime.date | None=None) -> str:
        name = 'GENERAL_MARKET_NEWS' if self.product is None else self.product.name
        if date is None:
            filename = name
        else:
            filename = '_'.join([name, str(date)])
        return filename + self.file_extension

    def create_storage_path(self, date: datetime.date | None=None) -> Path:
        product_type = 'NONE' if self.product is None else self.product.type.value
        product_name = 'NONE' if self.product is None else self.product.name
        if date is None:
            return (
                Path(f'env={self.env.value}')
                / f'data_source={self.data_source.name}'
                / f'data_origin={self.data_origin}'
                / f'product_type={product_type}'
            )            
        else:
            year, month, day = str(date).split('-')
            return (
                Path(f'env={self.env.value}')
                / f'data_source={self.data_source.name}'
                / f'data_origin={self.data_origin}'
                / f'product_type={product_type}'
                / f'product={product_name}'
                / f'year={year}'
                / f'month={month}'
                / f'day={day}'
            )

    def create_data_handler(
        self, 
        data_layer: DataLayer,
        data_path: str,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None = None,
        use_deltalake: bool = False,                        
    ) -> NewsDataHandler:
        return NewsDataHandler(
            data_model=self, 
            data_layer=data_layer,
            data_path=data_path, 
            filesystem=filesystem, 
            storage_options=storage_options, 
            use_deltalake=use_deltalake
        )

    def to_metadata(self) -> dict:
        return {
            **super().to_metadata(),
            'product': self.product.name if self.product else None,
            'product_type': self.product.type.value if self.product else None,
        }
