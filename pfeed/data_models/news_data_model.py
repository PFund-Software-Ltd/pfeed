from typing import Literal, Any
from pathlib import Path

from pydantic import model_validator

from pfund.products.product_base import BaseProduct
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


class NewsDataModel(TimeBasedDataModel):
    product: BaseProduct | None = None  # when news_type is 'general', product is not needed
    news_type: Literal['general', 'specific'] = ''
    file_extension: str = '.parquet'
    compression: str = 'snappy'

    def __str__(self):
        return ':'.join([super().__str__(), repr(self.product), f'{self.news_type}_news'])
    
    def model_post_init(self, __context: Any) -> None:
        if self.product is None:
            self.news_type = 'general'
        else:
            self.news_type = 'specific'
    
    @model_validator(mode='before')
    @classmethod
    def check_and_convert(cls, data: dict) -> dict:
        if 'product' in data:
            product = data['product']
            assert isinstance(product, BaseProduct), f'product must be a Product object, got {type(product)}'
        return data
    
    def _create_filename(self) -> str:
        if self.news_type == 'general':
            filename = '_'.join(["general_news", str(self.date)])
        elif self.news_type == 'specific':
            filename = '_'.join([self.product.name, str(self.date)])
        return filename + self.file_extension

    def _create_storage_path(self) -> Path:
        year, month, day = str(self.date).split('-')
        if self.news_type == 'general':
            return (
                Path(self.env.value)
                / self.data_source.name
                / self.data_origin
                / self.news_type.upper()
                / year
                / month 
                / day
            )
        elif self.news_type == 'specific':
            return (
                Path(self.env.value)
                / self.data_source.name
                / self.data_origin
                / self.product.type.value
                / self.product.name
                / year
                / month 
                / day
            )
