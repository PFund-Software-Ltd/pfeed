"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, Awaitable
if TYPE_CHECKING:
    import datetime
    import pandas as pd
    from pfund.exchanges.bybit.exchange import tProductCategory
    from pfund.datas.resolution import Resolution
    from pfund.typing import FullDataChannel
    from pfeed.typing import GenericFrame
    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.typing import tStorage, tDataLayer

from pfund.products.product_bybit import BybitProduct
from pfeed.feeds.crypto_market_feed import CryptoMarketFeed
from pfeed.sources.bybit.mixin import BybitMixin


__all__ = ['BybitMarketFeed']


class BybitMarketFeed(BybitMixin, CryptoMarketFeed):
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
        data_layer: tDataLayer='CLEANED',
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
        data = self.data_source.batch_api.get_data(data_model.product, data_model.date)
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
    
    async def _stream_impl(self, faucet_streaming_callback: Callable[[str, dict], Awaitable[None] | None]):
        async def _callback(msg: dict):
            channel_key = 'topic'  # tell faucet which key to use to get the channel
            await faucet_streaming_callback(channel_key, msg)
        self.data_source.stream_api.set_callback(_callback)
        await self.data_source.stream_api.connect()
        
    async def _close_stream(self):
        await self.data_source.stream_api.disconnect()
    
    def _add_default_transformations_to_stream(self, product: BybitProduct, resolution: Resolution):
        from pfeed.utils.utils import lambda_with_name
        self.transform(
            lambda_with_name('parse_message', lambda msg: BybitMarketFeed._parse_message(product, msg)),
        )
        super()._add_default_transformations_to_stream(product, resolution)
        
    @staticmethod
    def _parse_message(product: BybitProduct, msg: dict) -> dict:
        from pfund.exchanges.bybit.ws_api import WebsocketApi
        from pfund.exchanges.bybit.ws_api_bybit import BybitWebsocketApi
        BybitWebsocketApiClass: type[BybitWebsocketApi] = WebsocketApi._get_api_class(product.category)
        return BybitWebsocketApiClass._parse_message(msg)
    
    def _add_data_channel(self, product: BybitProduct, resolution: Resolution) -> str:
        return self.data_source.stream_api._add_data_channel(product, resolution)

    def add_channel(
        self, 
        channel: FullDataChannel, 
        *,
        channel_type: Literal['public', 'private'],
        category: tProductCategory | None = None,
    ):
        '''
        Args:
            channel: A channel to subscribe to.
                The channel specified will be used directly for the external data source, no modification will be made.
                Useful for subscribing channels not related to resolution.
        '''
        self.data_source.stream_api.add_channel(channel, channel_type=channel_type, category=category)
