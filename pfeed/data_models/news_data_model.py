from pathlib import Path

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
            filename = '_'.join(["GENERAL_MARKET_NEWS", str(self.date)])
        else:
            filename = '_'.join([self.product.name, str(self.date)])
        return filename + self.file_extension

    def _create_storage_path(self) -> Path:
        year, month, day = str(self.date).split('-')
        if self.product is None:
            return (
                Path(f'env={self.env.value}')
                / f'data_source={self.data_source.name}'
                / f'data_origin={self.data_origin}'
                / 'product_type=NONE'
                / 'product=NONE'
                / f'year={year}'
                / f'month={month}'
                / f'day={day}'
            )
        else:
            return (
                Path(f'env={self.env.value}')
                / f'data_source={self.data_source.name}'
                / f'data_origin={self.data_origin}'
                / f'product_type={self.product.type.value}'
                / f'product={self.product.name}'
                / f'year={year}'
                / f'month={month}'
                / f'day={day}'
            )
