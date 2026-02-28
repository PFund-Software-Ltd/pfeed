"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, ClassVar, Any, cast
if TYPE_CHECKING:
    from collections.abc import Coroutine
    import datetime
    import pandas as pd
    from pfund.datas.resolution import Resolution
    from pfeed.typing import GenericFrame
    from pfeed.storages.storage_config import StorageConfig
    
from pfund.entities.products.product_bybit import BybitProduct
from pfeed.sources.bybit.mixin import BybitMixin
from pfeed.sources.bybit.market_data_model import BybitMarketDataModel
from pfeed.enums import MarketDataType
from pfeed.feeds.market_feed import MarketFeed
from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin, WebSocketName, Message, ChannelKey


__all__ = ['BybitMarketFeed']


class BybitMarketFeed(StreamingFeedMixin, BybitMixin, MarketFeed):
    data_model_class: ClassVar[type[BybitMarketDataModel]] = BybitMarketDataModel
    date_columns_in_raw_data: ClassVar[list[str]] = ['timestamp']
    
    @staticmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        '''Normalize raw Bybit DataFrame into a consistent format.

        Args:
            df: DataFrame after `_standardize_date_column` (Bybit's 'timestamp' column renamed to 'date').

        Returns:
            Normalized DataFrame with:
            - 'size' renamed to 'volume'
            - 'side' mapped from Buy/Sell (case-insensitive) to 1/-1
            - volume cast to float64
        '''
        # some products use "Buy"/"Sell" while some use "buy"/"sell"
        df['side'] = df['side'].str.lower()
        MAPPING_COLS: dict[str, int] = {'buy': 1, 'sell': -1}
        RENAMING_COLS: dict[str, str] = {'size': 'volume'}
        df = df.rename(columns=RENAMING_COLS)
        df['side'] = df['side'].map(MAPPING_COLS)
        # Convert volume column to float64 to pass pandera schema validation, since inverse products have int volume
        df["volume"] = df["volume"].astype("float64")
        return df
    
    def download(
        self,
        product: str, 
        resolution: Resolution | MarketDataType | str='tick',
        rollback_period: Resolution | str | Literal['ytd', 'max']='1d',
        start_date: datetime.date | str='',
        end_date: datetime.date | str='',
        storage_config: StorageConfig | None=None,
        clean_data: bool=True,
        **product_specs: Any
    ) -> GenericFrame | None | BybitMarketFeed:
        '''Download historical data from Bybit.

        Args:
            product: product basis, e.g. BTC_USDT_PERP, ETH_USDT_OPT.
                Details of specifications should be specified in `product_specs`.
            resolution: Data resolution, e.g. '1m', '1h', 'tick'. Default is 'tick'.
            rollback_period: Period to rollback from today, only used when `start_date` is not specified.
                Accepts a resolution string, 'ytd', or 'max'. Default is '1d'.
            start_date: Start date.
                If not specified, rollback_period is used to determine the start date.
                Special case: if rollback_period='max', uses the data source's start_date attribute.
            end_date: End date. If not specified, use today's date.
            storage_config: Storage configuration.
                if None, downloaded data will NOT be stored to storage.
                if provided, downloaded data will be stored to storage based on the storage config.
            clean_data: Whether to clean raw data after download.
                If storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
                If True, downloaded raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
                If False, downloaded raw data will be returned as is.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                download(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.

        Returns:
            Downloaded data as a DataFrame, or None if no data is available.
            Returns self if used in pipeline mode (i.e. after calling `.pipeline()`).
        '''
        return super().download(  # pyright: ignore[reportReturnType]
            product=product,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            dataflow_per_date=True,
            storage_config=storage_config,
            clean_data=clean_data,
            **product_specs
        )

    def _download_impl(self, data_model: BybitMarketDataModel) -> bytes | pd.DataFrame | None:
        self.logger.debug(f'downloading {data_model}')
        data = self.batch_api.get_data(data_model.product, data_model.resolution, data_model.date)
        self.logger.debug(f'downloaded {data_model}')
        return data
    
    async def _stream_impl(
        self, 
        faucet_callback: Callable[[WebSocketName, Message, ChannelKey | None], Coroutine[Any, Any, None]]
    ):
        async def _callback(ws_name: WebSocketName, msg: Message):
            if 'topic' in msg:
                channel: str = msg['topic']
                category = ws_name.split('_')[1]
                category = BybitProduct.ProductCategory[category.upper()]
                channel_key: tuple[str, str] = self.stream_api.generate_channel_key(category, channel)
                # NOTE: Bybit's tick (publicTrade) messages contain multiple trades in a single message,
                # split them into individual messages so each tick flows through the pipeline separately
                if channel.startswith('publicTrade') and isinstance(msg.get('data'), list):
                    for item in msg['data']:
                        individual_msg = {**msg, 'data': item}
                        await faucet_callback(ws_name, individual_msg, channel_key)
                    return
            else:
                channel_key: tuple[str, str] | None = None
            await faucet_callback(ws_name, msg, channel_key)
        self.stream_api.set_callback(_callback)
        await self.stream_api.connect()
        
    async def _close_stream(self):
        await self.stream_api.disconnect()
    
    def _get_default_transformations_for_stream(self) -> list[Callable[..., Any]]:
        from pfeed.utils import lambda_with_name
        from pfeed.requests import MarketFeedStreamRequest
        request: MarketFeedStreamRequest = cast(MarketFeedStreamRequest, self._current_request)
        default_transformations = MarketFeed._get_default_transformations_for_stream(self)
        if request.clean_data:
            product: BybitProduct = cast(BybitProduct, request.product)
            default_transformations = [
                lambda_with_name(
                    'parse_message', 
                    lambda msg: BybitMarketFeed._parse_message(product, msg)  # pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType]
                ),
            ] + default_transformations
        return default_transformations
        
    @staticmethod
    def _parse_message(product: BybitProduct, msg: Message) -> Message:
        from pfund.brokers.crypto.exchanges.bybit.ws_api import WebSocketAPI
        from pfund.brokers.crypto.exchanges.bybit.ws_api_bybit import BybitWebSocketAPI
        assert product.category is not None, 'product.category is not initialized'
        BybitWebSocketAPIClass: type[BybitWebSocketAPI] = WebSocketAPI._get_api_class(product.category)
        return BybitWebSocketAPIClass._parse_message(msg)
