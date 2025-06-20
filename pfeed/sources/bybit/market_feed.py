"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import datetime
    import pandas as pd
    from pfeed.typing import GenericFrame
    from pfund.datas.resolution import Resolution
    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.typing import tStorage, tDataLayer

from pfeed.feeds.crypto_market_feed import CryptoMarketFeed


__all__ = ['BybitMarketFeed']


class BybitMarketFeed(CryptoMarketFeed):
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
        RENAMING_COLS = {'timestamp': 'date', 'size': 'volume'}
        df = df.rename(columns=RENAMING_COLS)
        df['side'] = df['side'].map(MAPPING_COLS)
        return df

    def download(
        self,
        product: str, 
        resolution: Resolution | str | Literal['tick', 'second', 'minute', 'hour', 'day', 'max']='tick',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        data_layer: Literal['RAW', 'CLEANED']='CLEANED',
        to_storage: tStorage | None='LOCAL',
        storage_options: dict | None=None,
        auto_transform: bool=True,
        **product_specs
    ) -> GenericFrame | None | dict[datetime.date, GenericFrame | None] | BybitMarketFeed:
        '''
        Args:
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                download(
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
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            to_storage=to_storage,
            storage_options=storage_options,
            auto_transform=auto_transform,
            dataflow_per_date=True,
            **product_specs
        )

    def _download_impl(self, data_model: MarketDataModel) -> bytes | None:
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
        data_origin: str='',
        data_layer: tDataLayer | None=None,
        data_domain: str='',
        from_storage: tStorage | None=None,
        to_storage: tStorage | None=None,
        storage_options: dict | None=None,
        force_download: bool=False,
        retrieve_per_date: bool=False,
        **product_specs
    ) -> GenericFrame | None | BybitMarketFeed:
        return super().get_historical_data(
            product=product,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            data_layer=data_layer,
            data_domain=data_domain,
            from_storage=from_storage,
            to_storage=to_storage,
            storage_options=storage_options,
            force_download=force_download,
            retrieve_per_date=retrieve_per_date,
            **product_specs
        )
        
    def stream(
        self,
        product: str | list[str] | list[list[str]] | None=None,
        channels: str | list[str] | None=None,
        **product_specs
    ) -> BybitMarketFeed:

        return self
    
    # TODO
    def group_streaming_products(self, product_groups: list[list[str]]):
        """
        Args:
            product_groups: A list of product groups. Format determines how data transformation is parallelized.
                All product data is still received by a single WebSocket manager using asyncio, 
                but each group will have its data dispatched to a separate Ray process 
                for parallel transformation (CPU-bound processing).
                
        Note:
            - WebSocket I/O is centralized and handled by a single asyncio event loop.
            - Ray is only used to parallelize CPU-bound data transformation after receipt.
        """
        if len(product_groups) >= 1:
            assert self._use_ray, '"use_ray" must be True when streaming multiple products in parallel'
        # TODO: check if number of cpus is enough for streaming
    
    # TODO
    def _stream_impl(
        self, 
        data_model: MarketDataModel, 
    ):
        # TODO: start streaming
        pass