from __future__ import annotations
from typing_extensions import TypedDict
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfund.typing import tEnvironment, ResolutionRepr
    from pfeed.enums import DataLayer
    from pfeed.typing import tDataSource

import datetime
from pathlib import Path

from pydantic import model_validator

from pfund.datas.resolution import Resolution
from pfund.products.product_base import BaseProduct
from pfeed.data_models.time_based_data_model import TimeBasedDataModel
from pfeed.data_handlers import MarketDataHandler


class MarketMetadata(TypedDict, total=True):
    env: tEnvironment
    data_source: tDataSource
    data_origin: str
    start_date: datetime.date
    end_date: datetime.date
    symbol: str
    resolution: ResolutionRepr
    asset_type: str


# metadata for delta table
class MarketDeltaMetadata(TypedDict, total=True):
    env: tEnvironment
    data_source: tDataSource
    data_origin: str
    dates: list[datetime.date]
    symbol: str
    resolution: ResolutionRepr
    asset_type: str


class MarketDataModel(TimeBasedDataModel):
    '''
    Args:
        symbol: unique identifier for the product.
        asset_type: asset type of the product. e.g. 'PERPETUAL', 'STOCK'.
        resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
            Default is '1d' = 1 day.
    '''
    product: BaseProduct
    resolution: Resolution
    file_extension: str = '.parquet'
    compression: str = 'snappy'
    
    def __str__(self):
        return ':'.join([super().__str__(), self.product.name, repr(self.resolution)])
    
    # FIXME: this is not needed anymore?
    @model_validator(mode='before')
    @classmethod
    def check_and_convert(cls, data: dict) -> dict:
        product = data['product']
        assert isinstance(product, BaseProduct), f'product must be a Product object, got {type(product)}'
        # convert resolution to Resolution object if it is a string
        resolution = data['resolution']
        if isinstance(resolution, str):
            resolution = Resolution(resolution)
        data['resolution'] = resolution
        return data
    
    def update_resolution(self, resolution: Resolution) -> None:
        self.resolution = resolution

    def create_filename(self, date: datetime.date) -> str:
        filename = '_'.join([self.product.key, str(date)])
        return filename + self.file_extension

    def create_storage_path(self, date: datetime.date) -> Path:
        path = (
            Path(f'env={self.env.value}')
            / f'data_source={self.data_source.name}'
            / f'data_origin={self.data_origin}'
            / f'asset_type={self.product.asset_type}'
            / f'symbol={self.product.symbol}'
            / f'resolution={repr(self.resolution)}'
        )
        if self.use_deltalake:
            return path
        else:
            year, month, day = str(date).split('-')
            return path / f'year={year}' / f'month={month}' / f'day={day}'

    def create_data_handler(
        self, 
        data_layer: DataLayer,
        data_path: str,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None = None,
    ) -> MarketDataHandler:
        return MarketDataHandler(
            data_model=self, 
            data_layer=data_layer,
            data_path=data_path, 
            filesystem=filesystem, 
            storage_options=storage_options, 
            use_deltalake=self.use_deltalake
        )

    def to_metadata(self) -> MarketMetadata:
        return {
            **super().to_metadata(),
            'symbol': self.product.symbol,
            'resolution': repr(self.resolution),
            'asset_type': str(self.product.asset_type),
        }