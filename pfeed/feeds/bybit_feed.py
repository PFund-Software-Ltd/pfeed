"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import datetime
    from pfund.types.literals import tCEFI_PRODUCT_TYPE
    from pfeed.types.core import tDataFrame
    from pfund.datas.resolution import Resolution
    from pfund.products.product_base import BaseProduct

    from pfeed.types.core import tDataModel
    from pfeed.types.literals import tSTORAGE
    from pfeed.flows.dataflow import DataFlow
    from pfeed.const.enums import DataRawLevel

import pandas as pd

from pfeed.feeds.base_feed import clear_current_dataflows
from pfeed.feeds.crypto_market_data_feed import CryptoMarketDataFeed


__all__ = ['BybitFeed']


class BybitFeed(CryptoMarketDataFeed):
    @staticmethod
    def get_data_source():
        from pfeed.sources.bybit.source import BybitSource
        return BybitSource()
    
    @staticmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw data from Bybit API into standardized format.

        This method performs the following normalizations:
        - Renames columns to standard names (timestamp -> ts, size -> volume)
        - Maps trade side values from Buy/Sell to 1/-1
        - Corrects reverse-ordered data if detected

        Args:
            df (pd.DataFrame): Raw DataFrame from Bybit API containing trade data

        Returns:
            pd.DataFrame: Normalized DataFrame with standardized column names and values
        """
        MAPPING_COLS = {'Buy': 1, 'Sell': -1}
        RENAMING_COLS = {'timestamp': 'ts', 'size': 'volume'}
        df = df.rename(columns=RENAMING_COLS)
        df['side'] = df['side'].map(MAPPING_COLS)
        is_in_reverse_order = df['ts'][0] > df['ts'][1]
        if is_in_reverse_order:
            df = df.iloc[::-1].reset_index(drop=True)
        return df
    
    # TODO
    @clear_current_dataflows
    def stream(self) -> BybitFeed:
        raise NotImplementedError(f'{self.name} stream() is not implemented')
        return self
    
    # TODO
    def _execute_stream(self, data_model: tDataModel):
        raise NotImplementedError(f'{self.name} _execute_stream() is not implemented')

    def download(
        self,
        products: str | list[str] | None=None, 
        product_types: tCEFI_PRODUCT_TYPE | list[tCEFI_PRODUCT_TYPE] | None=None, 
        data_type: Literal['tick', 'second', 'minute', 'hour', 'day']='tick',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        to_storage: tSTORAGE='local',
        filename_prefix: str='',
        filename_suffix: str='',
        product_specs: dict[str, dict] | None=None,  # {'product_basis': {'attr': 'value', ...}}
    ) -> BybitFeed:
        '''
        Args:
            filename_prefix: The prefix of the filename.
            filename_suffix: The suffix of the filename.
            product_specs: The specifications for the products.
                'BTC_USDT_OPT' is in `products`, you need to provide the specifications of the option in `product_specs`:
                e.g. {'BTC_USDT_OPT': {'strike_price': 10000, 'expiration': '2024-01-01', 'option_type': 'CALL'}}
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
        '''
        return super().download(
            products=products,
            symbols=None,
            product_types=product_types,
            data_type=data_type,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            raw_level=raw_level,
            to_storage=to_storage,
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
            product_specs=product_specs,
        )
    
    def _create_download_dataflows(
        self,
        product: BaseProduct,
        resolution: Resolution,
        raw_level: DataRawLevel,
        start_date: datetime.date,
        end_date: datetime.date,
        filename_prefix: str,
        filename_suffix: str,
    ) -> list[DataFlow]:
        from pfeed.utils.utils import get_dates_in_between
        dataflows: list[DataFlow] = []
        # NOTE: one data model per date
        for date in get_dates_in_between(start_date, end_date):
            data_model = self.create_market_data_model(product, resolution, raw_level, date, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
            # create a dataflow that schedules _execute_download()
            dataflow: DataFlow = super().extract('download', data_model)
            dataflows.append(dataflow)
        return dataflows

    def _execute_download(self, data_model: tDataModel) -> bytes | None:
        self.logger.debug(f'downloading {data_model}')
        data = self.api.get_data(data_model.product, data_model.date)
        self.logger.debug(f'downloaded {data_model}')
        return data

    def get_historical_data(
        self,
        product: str,
        resolution: str="1t",
        rollback_period: str="1d",
        start_date: str="",
        end_date: str="",
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        from_storage: tSTORAGE | None=None,
        **product_specs
    ) -> tDataFrame | None:
        return super().get_historical_data(
            product=product,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            raw_level=raw_level,
            from_storage=from_storage,
            **product_specs
        )