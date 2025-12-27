"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, Awaitable
if TYPE_CHECKING:
    import datetime
    import pandas as pd
    from pfund.exchanges.bybit.exchange import tProductCategory
    from pfund.datas.resolution import Resolution
    from pfund.typing import FullDataChannel, tEnvironment
    from pfeed.typing import GenericFrameOrNone
    from pfeed.sources.bybit.stream_api import ChannelKey
    from pfeed.typing import tStorage, tDataLayer

from pfund.products.product_bybit import BybitProduct
from pfeed.feeds.crypto_market_feed import CryptoMarketFeed
from pfeed.sources.bybit.mixin import BybitMixin
from pfeed.sources.bybit.market_data_model import BybitMarketDataModel
from pfeed.enums import DataLayer


__all__ = ['BybitMarketFeed']


class BybitMarketFeed(BybitMixin, CryptoMarketFeed):
    @property
    def data_model_class(self) -> type[BybitMarketDataModel]:
        return BybitMarketDataModel
    
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
        df['side'] = df['side'].str.lower()  # some products use "Buy"/"Sell" while some use "buy"/"sell"
        MAPPING_COLS = {'buy': 1, 'sell': -1}
        RENAMING_COLS = {'timestamp': 'date', 'size': 'volume'}
        df = df.rename(columns=RENAMING_COLS)
        df['side'] = df['side'].map(MAPPING_COLS)
        # Convert volume column to float64 to pass pandera schema validation, since inverse products have int volume
        df["volume"] = df["volume"].astype("float64")
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
        **product_specs
    ) -> GenericFrameOrNone | dict[datetime.date, GenericFrameOrNone] | BybitMarketFeed:
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
            dataflow_per_date=True,
            include_metadata=False,
            **product_specs
        )

    def _download_impl(self, data_model: BybitMarketDataModel) -> bytes | None:
        self.logger.debug(f'downloading {data_model}')
        data = self.data_source.batch_api.get_data(data_model.product, data_model.resolution, data_model.date)
        self.logger.debug(f'downloaded {data_model}')
        return data
    
    async def _stream_impl(self, faucet_streaming_callback: Callable[[str, dict, BybitMarketDataModel | None], Awaitable[None] | None]):
        stream_api = self.data_source.stream_api
        async def _callback(ws_name: str, msg: dict):
            if channel := msg.get('topic', None):
                category = ws_name.split('_')[1]
                category = BybitProduct.ProductCategory[category.upper()]
                channel_key: ChannelKey = stream_api.generate_channel_key(category, channel)
                data_model = stream_api._streaming_bindings[channel_key]
            else:
                data_model = None
            await faucet_streaming_callback(ws_name, msg, data_model)
        stream_api.set_callback(_callback)
        await stream_api.connect()
        
    async def _close_stream(self):
        await self.data_source.stream_api.disconnect()
    
    def _add_default_transformations_to_stream(self, data_layer: DataLayer, product: BybitProduct, resolution: Resolution):
        from pfeed.utils import lambda_with_name
        if data_layer != DataLayer.RAW:
            self.transform(
                lambda_with_name('parse_message', lambda msg: BybitMarketFeed._parse_message(product, msg)),
            )
        super()._add_default_transformations_to_stream(data_layer, product, resolution)
        
    @staticmethod
    def _parse_message(product: BybitProduct, msg: dict) -> dict:
        from pfund.exchanges.bybit.ws_api import WebSocketAPI
        from pfund.exchanges.bybit.ws_api_bybit import BybitWebSocketAPI
        BybitWebSocketAPIClass: type[BybitWebSocketAPI] = WebSocketAPI._get_api_class(product.category)
        return BybitWebSocketAPIClass._parse_message(msg)
    
    def _add_data_channel(self, data_model: BybitMarketDataModel) -> FullDataChannel:
        if data_model.env != self.data_source.stream_api.env:
            self.data_source.create_stream_api(env=data_model.env)
        return self.data_source.stream_api._add_data_channel(data_model)

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

    # TODO: use data_source.batch_api
    def _fetch_impl(self, data_model: BybitMarketDataModel, *args, **kwargs) -> GenericFrameOrNone:
        raise NotImplementedError(f'{self.name} _fetch_impl() is not implemented')
    
    # DEPRECATED
    # def get_historical_data(
    #     self,
    #     product: str,
    #     resolution: Resolution | str | Literal['tick', 'second', 'minute', 'hour', 'day']="1tick",
    #     rollback_period: str="1day",
    #     start_date: str="",
    #     end_date: str="",
    #     data_origin: str='',
    #     data_layer: tDataLayer | None=None,
    #     data_domain: str='',
    #     from_storage: tStorage | None=None,
    #     to_storage: tStorage | None=None,
    #     storage_options: dict | None=None,
    #     force_download: bool=False,
    #     retrieve_per_date: bool=False,
    #     **product_specs
    # ) -> GenericFrameOrNone | BybitMarketFeed:
    #     return super().get_historical_data(
    #         product=product,
    #         resolution=resolution,
    #         rollback_period=rollback_period,
    #         start_date=start_date,
    #         end_date=end_date,
    #         data_origin=data_origin,
    #         data_layer=data_layer,
    #         data_domain=data_domain,
    #         from_storage=from_storage,
    #         to_storage=to_storage,
    #         storage_options=storage_options,
    #         force_download=force_download,
    #         retrieve_per_date=retrieve_per_date,
    #         **product_specs
    #     )