from pathlib import Path
import datetime

from pydantic import model_validator

from pfund.products.product_base import BaseProduct
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


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
    
    def _create_filename(self) -> str:
        if self.product is None:
            filename = '_'.join(["general_news", str(self.date)])
        else:
            filename = '_'.join([self.product.name, str(self.date)])
        return filename + self.file_extension

    def _create_storage_path(self) -> Path:
        year, month, day = str(self.date).split('-')
        if self.product is None:
            return (
                Path(self.env.value)
                / self.data_source.name
                / self.data_origin
                / 'GENERAL'
                / 'NEWS'
                / year
                / month 
                / day
            )
        else:
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
