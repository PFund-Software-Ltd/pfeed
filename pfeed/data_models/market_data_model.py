from typing import Any
from pathlib import Path

from pydantic import model_validator
from pfund.datas.resolution import Resolution

from pfeed.data_models.time_based_data_model import TimeBasedDataModel


class MarketDataModel(TimeBasedDataModel):
    '''
    Args:
        product:  e.g. BTC_USDT_PERP, AAPL_USD_STK.
        product_type: The type of the product. e.g. 'PERP' | 'STK'.
        resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
            Default is '1d' = 1 day.
    '''
    product: str
    resolution: Resolution
    product_type: str = ''
    file_extension: str = '.parquet'
    compression: str = 'zstd'
    
    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        self.product_type = self.product.split('_')[2]
    
    def __str__(self):
        return '_'.join([super().__str__(), self.product, str(self.resolution)])

    @model_validator(mode='after')
    def validate(self):
        self.validate_resolution()
        self.validate_product()
        return self
    
    def validate_resolution(self):
        if hasattr(self.source, 'highest_resolution'):
            assert self.resolution <= self.source.highest_resolution, f'{self.resolution=} is not supported for {self.source.name}'
        return self.resolution

    def validate_product(self):
        import re
        # REVIEW: pattern: XXX_YYY_PTYPE, hard-coded the max length of XXX and YYY is 8
        pdt_pattern = re.compile(r'^[A-Za-z]{1,8}_[A-Za-z]{1,8}_(.+)$')
        match = pdt_pattern.match(self.product)
        if not match or match.group(1) not in self.source.product_types:
            raise ValueError(f'{self.product} is not a valid product for {self.source.name}')
        return self.product

    def __hash__(self):
        return hash((self.source.name, self.unique_identifier, self.date, self.product, self.resolution))

    def create_filename(self) -> str:
        filename = self.product + '_' + str(self.date)
        if self.filename_prefix:
            filename = self.filename_prefix + '_' + filename
        if self.filename_suffix:
            filename += '_' + self.filename_suffix
        filename += self.file_extension
        return filename

    def create_storage_path(self) -> Path:
        year, month, day = str(self.date).split('-')
        return (
            Path(self.env.value)
            / self.source.name
            / self.unique_identifier
            / self.product_type
            / self.product
            / str(self.resolution)
            / year
            / month 
            / self.filename
        )
