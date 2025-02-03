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
    ) -> tDataFrame | None | dict[datetime.date, tDataFrame | None] | BybitFeed:
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
        resolution: Resolution | str | Literal['tick', 'second', 'minute', 'hour', 'day']="1tick",
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
        
    def retrieve(
        self,
        product: str,
        resolution: Resolution | str | Literal['tick', 'second', 'minute', 'hour', 'day'],
        rollback_period: str="1w",
        start_date: str='',
        end_date: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        from_storage: tSTORAGE | None=None,
        auto_transform: bool=True,
        storage_configs: dict | None=None,
        concat_output: bool=True,
        **product_specs
    ) -> tDataFrame | None | dict[datetime.date, tDataFrame | None] | BybitFeed:
        '''Retrieve data from storage.
        Args:
            product: Financial product, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
                For convenience, data types such as 'tick', 'second', 'minute' etc. are also supported.
            rollback_period:
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1w' = 1 week.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use yesterday's date as the default start date.
            end_date: End date.
                If not specified, use today's date as the end date.
            data_layer:
                'curated' (least raw): normalize data (refer to 'cleaned' below), also remove all non-standard columns
                    e.g. standard columns in second data are ts, product, open, high, low, close, volume
                'cleaned' (default): perform normalization following pfund's convention, preserve all columns
                    Normalization example:
                    - renaming: 'timestamp' -> 'ts'
                    - mapping: 'buy' -> 1, 'sell' -> -1
                'raw' (most raw): keep the original data, no transformation will be performed.
                It will be ignored if the data is loaded from storage but not downloaded.
            data_domain: The domain of the data. e.g. 'market_data'. Can be a custom domain.
            from_storage: try to load data from this storage.
                If not specified, will search through all storages, e.g. local, minio, cache.
                If no data is found, will try to download the missing data from the data source.
            auto_transform: Whether to apply default transformations to the data.
                Default transformations include:
                - resampling data to the target resolution
            concat_output: Whether to concatenate the data from different dates.
                If True, the data from different dates will be concatenated into a single DataFrame.
                If False, the data from different dates will be returned as a dictionary of DataFrames with date as the key.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                retrieve(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
        '''
        return super().retrieve(
            product=product,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            from_storage=from_storage,
            auto_transform=auto_transform,
            storage_configs=storage_configs,
            concat_output=concat_output,
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