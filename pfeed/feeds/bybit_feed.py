"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import datetime
    from bytewax.dataflow import Stream as BytewaxStream
    from bytewax.inputs import Source as BytewaxSource
    from pfeed.typing.core import tDataFrame
    from pfund.datas.resolution import Resolution
    from pfund.products.product_base import BaseProduct

    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.typing.literals import tSTORAGE, tDATA_LAYER
    from pfeed.flows.dataflow import DataFlow

import pandas as pd

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

    def download(
        self,
        product: str, 
        resolution: Resolution | str | Literal['tick', 'second', 'minute', 'hour', 'day']='tick',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        to_storage: tSTORAGE='local',
        auto_transform: bool=True,
        **product_specs
    ) -> BybitFeed:
        '''
        Args:
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                get_historical_data(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
        '''
        return super().download(
            product=product,
            resolution=resolution,
            symbols=None,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            to_storage=to_storage,
            auto_transform=auto_transform,
            **product_specs
        )
    
    def _create_download_dataflows(
        self,
        product: BaseProduct,
        unit_resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        data_origin: str='',
    ) -> list[DataFlow]:
        assert unit_resolution.period == 1, 'unit_resolution must have period = 1'
        dataflows: list[DataFlow] = []
        # NOTE: one data model per date
        for date in pd.date_range(start_date, end_date).date:
            data_model = self.create_data_model(product, unit_resolution, date, data_origin=data_origin)
            # create a dataflow that schedules _execute_download()
            dataflow = self._extract_download(data_model)
            dataflows.append(dataflow)
        return dataflows

    def _execute_download(self, data_model: MarketDataModel) -> bytes | None:
        self.logger.debug(f'downloading {data_model}')
        data = self.api.get_data(data_model.product, data_model.date)
        self.logger.debug(f'downloaded {data_model}')
        return data

    def get_historical_data(
        self,
        product: str,
        resolution: str="1tick",
        rollback_period: str="1day",
        start_date: str="",
        end_date: str="",
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        data_origin: str='',
        from_storage: tSTORAGE | None=None,
        **product_specs
    ) -> tDataFrame | None | BybitFeed:
        return super().get_historical_data(
            product,
            resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            data_origin=data_origin,
            from_storage=from_storage,
            **product_specs
        )
        
    # TODO
    def stream(self) -> BybitFeed:
        raise NotImplementedError(f'{self.name} stream() is not implemented')
        return self
    
    # TODO
    def _execute_stream(
        self, 
        data_model: MarketDataModel, 
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
    ):
        if self._use_bytewax:
            if bytewax_source is None:
                # TODO: derive bytewax source based on the feed
                pass
            return bytewax_source
        else:
            # TODO: start streaming
            pass