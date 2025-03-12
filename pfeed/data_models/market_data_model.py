import datetime
from pathlib import Path

from pydantic import model_validator
from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct

from pfeed.data_models.time_based_data_model import TimeBasedDataModel


class MarketDataModel(TimeBasedDataModel):
    '''
    Args:
        product:  e.g. BTC_USDT_PERP, AAPL_USD_STK.
        product_type: The type of the product. e.g. 'PERP' | 'STK'.
        resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
            Default is '1d' = 1 day.
    '''
    product: BaseProduct
    resolution: Resolution
    file_extension: str = '.parquet'
    compression: str = 'snappy'
    
    def __str__(self):
        return ':'.join([super().__str__(), repr(self.product), str(self.resolution)])
    
    @model_validator(mode='before')
    @classmethod
    def check_and_convert(cls, data: dict) -> dict:
        if 'product' in data:
            product = data['product']
            assert isinstance(product, BaseProduct), f'product must be a Product object, got {type(product)}'
        if 'resolution' in data:
            # convert resolution to Resolution object if it is a string
            resolution = data['resolution']
            if isinstance(resolution, str):
                resolution = Resolution(resolution)
            data['resolution'] = resolution
        return data
    
    @model_validator(mode='after')
    def validate(self):
        self._validate_resolution()
        return self
    
    @property
    def global_min_resolution(self) -> Resolution:
        return Resolution('1d')
    
    def _validate_resolution(self):
        '''Validates the resolution of the data model.
        Resolution must be >= '1d' and <= the highest resolution supported by the data source.
        '''
        # lowest_supported_resolution = Resolution('1' + [dt.name for dt in MarketDataType][-1])
        lowest_supported_resolution = self.global_min_resolution
        assert lowest_supported_resolution <= self.resolution <= self.data_source.highest_resolution, f'{self.resolution=} is not supported for {self.data_source.name}'
        return self.resolution

    def update_resolution(self, resolution: Resolution) -> None:
        self.resolution = resolution
        self._validate_resolution()
        self.storage_path = self._create_storage_path()

    def _create_filename_per_date(self, date: datetime.date) -> str:
        filename = '_'.join([self.product.name, str(date)])
        return filename + self.file_extension

    def _create_storage_path_per_date(self, date: datetime.date) -> Path:
        year, month, day = str(date).split('-')
        return (
            Path(f'env={self.env.value}')
            / f'data_source={self.data_source.name}'
            / f'data_origin={self.data_origin}'
            / f'product_type={self.product.type.value}'
            / f'product={self.product.name}'
            / f'resolution={repr(self.resolution)}'
            / f'year={year}'
            / f'month={month}'
            / f'day={day}'
        )
