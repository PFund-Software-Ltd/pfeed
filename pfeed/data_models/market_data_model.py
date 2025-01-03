from pathlib import Path

from pydantic import model_validator
from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct

from pfeed.data_models.time_based_data_model import TimeBasedDataModel
from pfeed.const.enums import MarketDataType


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
    compression: str = 'zstd'
    
    def __str__(self):
        if self.unique_identifier != self.source.name.value:
            return ':'.join([super().__str__(), repr(self.product), str(self.resolution)])
        elif not self.end_date:
            return ':'.join([str(self.start_date), repr(self.product), str(self.resolution)])
        else:
            return ':'.join(['(from)' + str(self.start_date), '(to)' + str(self.end_date), repr(self.product), str(self.resolution)])
    
    @model_validator(mode='after')
    def validate(self):
        self.validate_resolution()
        return self
    
    def validate_resolution(self):
        # global lowest resolution is "1d" (daily data)
        global_lowest_resolution = Resolution('1' + [dt.name for dt in MarketDataType][-1])
        assert global_lowest_resolution <= self.resolution <= self.source.highest_resolution, f'{self.resolution=} is not supported for {self.source.name}'
        return self.resolution

    def __hash__(self):
        return hash((self.source.name, self.unique_identifier, self.start_date, self.end_date, self.product, self.resolution))

    def create_filename(self) -> str:
        # NOTE: since storage is per date, only uses self.date (start_date) to create filename
        filename = '_'.join([self.product.basis, str(self.date)])
        if self.filename_prefix:
            filename = self.filename_prefix + '_' + filename
        if self.filename_suffix:
            filename += '_' + self.filename_suffix
        filename += self.file_extension
        return filename

    def create_storage_path(self) -> Path:
        # NOTE: since storage is per date, only uses self.date (start_date) to create storage path
        year, month, day = str(self.date).split('-')
        return (
            Path(self.env.value)
            / self.source.name
            / self.unique_identifier
            / self.product.type.value
            / self.product.name
            / str(self.resolution)
            / year
            / month 
            / self.filename
        )
