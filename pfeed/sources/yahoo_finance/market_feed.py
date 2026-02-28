from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, ClassVar, Any, cast
if TYPE_CHECKING:
    from collections.abc import Coroutine, Awaitable
    import pandas as pd
    from yfinance import Ticker
    from pfund.datas.resolution import Resolution
    from pfund.entities.products.product_base import BaseProduct
    from pfeed.typing import GenericFrame
    from pfeed.storages.storage_config import StorageConfig
    from pfeed.sources.yahoo_finance.stream_api import ChannelKey

import time
import datetime

from pfund.enums import Environment
from pfeed.sources.yahoo_finance.mixin import YahooFinanceMixin
from pfeed.sources.yahoo_finance.market_data_model import YahooFinanceMarketDataModel
from pfeed.feeds.market_feed import MarketFeed
from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin, WebSocketName, Message, ChannelKey
from pfeed.enums import MarketDataType, StreamMode


__all__ = ["YahooFinanceMarketFeed"]


# NOTE: only yfinance's period='max' is used, everything else is converted to start_date and end_date
# i.e. any resampling inside yfinance (interval always ='1x') is not used, it's all done by pfeed
class YahooFinanceMarketFeed(StreamingFeedMixin, YahooFinanceMixin, MarketFeed):
    data_model_class: ClassVar[type[YahooFinanceMarketDataModel]] = YahooFinanceMarketDataModel
    # "Date" is used for daily data and "Datetime" is used for other resolutions in yfinance
    date_columns_in_raw_data: ClassVar[list[str]] = ['Datetime', 'Date']
    
    _ADAPTER: ClassVar[dict[str, dict[str, str]]] = {
        "timeframe": {
            # pfund's timeframe: yfinance's timeframe
            "w": "wk",
        }
    }

    # _URLS: ClassVar[dict[str, str]] = {
    #     "rest": "https://query1.finance.yahoo.com",
    #     "ws": "wss://streamer.finance.yahoo.com",
    # }

    # yfinance's valid periods = pfund's rollback_periods
    # SUPPORTED_ROLLBACK_PERIODS = {
    #     "d": [1, 5],
    #     "mo": [1, 3, 6],
    #     "y": [1, 2, 5, 10],
    # }
    
    # yfinance's valid intervals = pfund's resolutions
    # SUPPORTED_RESOLUTIONS = {
    #     "m": [1, 2, 5, 15, 30, 60, 90],
    #     "h": [1],
    #     "d": [1, 5],
    #     "w": [1],
    #     "mo": [1, 3],
    # }
    
    @staticmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        '''Normalize raw yfinance DataFrame into a consistent format.

        Args:
            df: DataFrame after `_standardize_date_column` (yfinance date column renamed to 'date').

        Returns:
            Normalized DataFrame with:
            - Lowercased column names with spaces replaced by underscores
            - 'stock_splits' renamed to 'splits'
            - volume cast to float64
        '''
        # convert column names to lowercase and replace spaces with underscores
        # if there are spaces in column names, they will be turned into some weird names like "_10" during "for row in df.itertuples()"
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

        RENAMING_COLS = {'stock_splits': 'splits'}
        df = df.rename(columns=RENAMING_COLS)
        
        # convert volume (int) to float
        df['volume'] = df['volume'].astype("float64")  # pyright: ignore[reportUnknownMemberType]
        return df
    
    def _handle_rollback_max_period(
        self, 
        resolution: Resolution | str | Literal['minute', 'hour', 'day'], 
        start_date: datetime.date | str, 
        end_date: datetime.date | str
    ):
        resolution: Resolution = self.create_resolution(resolution)
        if resolution.is_day():
            # HACK: use '1900-01-01' as the start date for daily data since we don't know the exact start date when rollback_period == 'max'
            start_date = '1900-01-01'
            end_date = ''
            rollback_period = ''
        elif resolution.is_hour():
            rollback_period = '2y'  # max is 2 years for hourly data
        elif resolution.is_minute():
            rollback_period = '8d'  # max is 8 days for minute data
        else:
            raise ValueError(f'{resolution} is not supported')
        return start_date, end_date, rollback_period

    def _check_yfinance_kwargs(self, yfinance_kwargs: dict[str, Any] | None) -> dict[str, Any]:
        yfinance_kwargs = yfinance_kwargs or {}
        assert "interval" not in yfinance_kwargs, "`interval` duplicates with pfeed's `resolution`, please remove it"
        assert "period" not in yfinance_kwargs, "`period` duplicates with pfeed's `rollback_period`, please remove it"
        assert "start" not in yfinance_kwargs, "`start` duplicates with pfeed's `start_date`, please remove it"
        assert "end" not in yfinance_kwargs, "`end` duplicates with pfeed's `end_date`, please remove it"
        return yfinance_kwargs
    
    def download(
        self,
        product: str,
        symbol: str='',
        resolution: Resolution | Literal['minute', 'hour', 'day'] | str='day',
        rollback_period: Resolution | str | Literal["ytd", "max"]='1week',
        start_date: datetime.date | str='',
        end_date: datetime.date | str='',
        storage_config: StorageConfig | None=None,
        clean_data: bool=True,
        yfinance_kwargs: dict[str, Any] | None=None,
        **product_specs: Any
    ) -> GenericFrame | None | YahooFinanceMarketFeed:
        '''
        Download historical data from Yahoo Finance.
        Be reminded that if you include today's data, it can be incomplete, this especially applies to the usage of rollback_period.
        e.g. rollback_period='ytd'/'max' includes today's data, and today is not finished, so the data is incomplete.
        start_date and end_date (not including today) should be specified to avoid this issue.

        Args:
            product: product basis, e.g. AAPL_USD_STK, BTC_USDT_PERP.
                Details of specifications should be specified in `product_specs`.
            symbol: Symbol used by yfinance's Ticker. If not specified, derived from `product`.
                Note that the derived symbol might NOT be correct, in that case, you should specify it manually.
            resolution: Data resolution, e.g. '1m', '1h', 'day'. Default is 'day'.
            rollback_period: Period to rollback from today, only used when `start_date` is not specified.
                Accepts a resolution string, 'ytd', or 'max'. Default is 'max'.
            start_date: Start date.
                If not specified, rollback_period is used to determine the start date.
                Special case: if rollback_period='max', see `_handle_rollback_max_period` for yfinance-specific logic.
            end_date: End date. If not specified, use today's date.
            storage_config: Storage configuration.
                if None, downloaded data will NOT be stored to storage.
                if provided, downloaded data will be stored to storage based on the storage config.
            clean_data: Whether to clean raw data after download.
                If storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
                If True, downloaded raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
                If False, downloaded raw data will be returned as is.
            yfinance_kwargs: Extra kwargs forwarded directly to yfinance's Ticker.history().
                refer to kwargs in history() in yfinance/scrapers/history.py
                Note: interval, period, start, end are managed by pfeed and must not be passed here.
            product_specs: The specifications for the product.
                if product is "AAPL_USD_OPT", you need to provide the specifications of the option as kwargs:
                download(
                    product='AAPL_USD_OPT',
                    strike_price=150,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.

        Returns:
            Downloaded data as a DataFrame, or None if no data is available.
            Returns self if used in pipeline mode (i.e. after calling `.pipeline()`).
        '''
        self._yfinance_kwargs: dict[str, Any] = self._check_yfinance_kwargs(yfinance_kwargs)
        if rollback_period == 'max' and not start_date:
            start_date, end_date, rollback_period = self._handle_rollback_max_period(resolution, start_date, end_date)
        return super().download(  # pyright: ignore[reportReturnType]
            product=product,
            symbol=symbol,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            dataflow_per_date=False,
            storage_config=storage_config,
            clean_data=clean_data,
            **product_specs
        )
    
    def _download_impl(self, data_model: YahooFinanceMarketDataModel) -> pd.DataFrame | None:
        # convert pfund's resolution format to yfinance's interval
        resolution = data_model.resolution
        timeframe = repr(resolution.timeframe)
        etimeframe = self._ADAPTER["timeframe"].get(timeframe, timeframe)
        eresolution = str(resolution.period) + etimeframe
        
        product = data_model.product
        symbol = product.symbol
        assert symbol, f'symbol is required for {data_model}'
        ticker = self.batch_api.Ticker(symbol)
        
        df: pd.DataFrame | None = None
        num_retries = 3
        # NOTE: yfinance's end_date is not inclusive, so we need to add 1 day to the end_date
        yfinance_end_date = data_model.end_date + datetime.timedelta(days=1)
        
        while num_retries:
            num_retries -= 1
            self.logger.debug(f'downloading {data_model}')
            # NOTE: yfinance's period is not used, only use start_date and end_date for data clarity in storage
            df: pd.DataFrame | None = ticker.history(
                interval=eresolution,
                start=str(data_model.start_date),
                end=str(yfinance_end_date),
                **self._yfinance_kwargs
            )
            if df is None or df.empty:
                if num_retries:
                    self.logger.info(f'failed to download {product.symbol} {resolution} data, retrying...')
                    time.sleep(1)
            else:
                self.logger.debug(f'downloaded {data_model}')
                # convert tz-aware index to UTC naive, then reset to column
                if hasattr(df.index, 'tz') and df.index.tz is not None:  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
                    df.index = df.index.tz_convert("UTC").tz_localize(None)  # pyright: ignore[reportUnknownMemberType, reportAttributeAccessIssue]
                if str(data_model.start_date) == '1900-01-01':  # rollback_period='max' for daily data
                    actual_start_date = min(df.index).date()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
                    data_model.update_start_date(actual_start_date)  # pyright: ignore[reportUnknownArgumentType]
                    self.logger.debug(f'got actual start date, set start_date={actual_start_date} for {data_model}')
                # NOTE: reset index here so date columns (e.g. 'Datetime', 'Date') are actual columns,
                # which is required by _standardize_date_column to find and rename them
                df = df.reset_index()
                break
        else:
            self.logger.warning(
                f'failed to download {product.symbol} {resolution} data after all retries, ' +
                f'please check if start_date={data_model.start_date} and end_date={data_model.end_date} is within a valid range. ' +
                'If it happens on trading days, it is possibly due to rate limit, network issue, or bugs in `pfeed` or `yfinance`, ' +
                'you may try to extend the time range or avoid using `rollback_period` to try again.'
            )
        self._yfinance_kwargs.clear()
        return df
    
    def stream(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str='1tick',
        symbol: str='',
        rollback_period: Resolution | str | Literal['ytd', 'max']='7d',
        start_date: datetime.date | str='',
        end_date: datetime.date | str='',
        callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None=None,
        data_origin: str='',
        env: Environment | str=Environment.LIVE,
        stream_mode: StreamMode | str=StreamMode.FAST,
        flush_interval: int=100,  # in seconds
        clean_data: bool=True,
        storage_config: StorageConfig | None=None,
        **product_specs: Any
    ) -> MarketFeed | None:
        '''Stream market data, either live or by replaying historical data.

        When `start_date`, `end_date`, or `rollback_period` is provided, the stream replays
        historical data for the specified date range before continuing with live data.
        When none are provided, only live data is streamed starting from today.

        Args:
            product: Financial product, e.g. BTC_USDT_PERP, AAPL_USD_STK.
                Details of specifications should be specified in `product_specs`.
            resolution: Data resolution, e.g. '1m', '1h', 'tick'.
            symbol: Symbol used by the data source. If not specified, derived from `product`.
                Note that the derived symbol might NOT be correct, in that case, specify it manually.
            rollback_period: Period to rollback from today for historical replay.
                Only used when `start_date` is not specified. Default is '1d'.
                Accepts a resolution string (e.g. '7d'), 'ytd' (year to date), or 'max'.
            start_date: Start date for historical replay.
                If not specified, `rollback_period` is used to determine the start date.
            end_date: End date for historical replay. If not specified, uses today's date.
            callback: Async or sync callable invoked for each incoming message.
                Receives the raw message dict. If None, messages are handled by the default pipeline.
            data_origin: Origin label for the data, used to distinguish data from different sources.
            env: Trading environment. Cannot be BACKTEST. Default is LIVE.
            stream_mode: SAFE or FAST.
                If FAST, streaming data is cached in memory before writing to disk —
                faster write speed but higher data loss risk on crash.
                If SAFE, streaming data is written to disk immediately —
                slower write speed but minimal data loss risk.
            flush_interval: Interval in seconds for flushing buffered streaming data to storage. Default is 100 seconds.
                If using deltalake:
                Frequent flushes reduce write performance and generate many small files
                (e.g. part-00001-0a1fd07c-9479-4a72-8a1e-6aa033456ce3-c000.snappy.parquet).
                Infrequent flushes create larger files but increase data loss risk during crashes when using FAST stream_mode.
                Fine-tune based on your actual use case.
            clean_data: Whether to clean raw streaming data.
                If storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
                If True, raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
                If False, raw data will be passed through as is.
            storage_config: Storage configuration.
                If None, streamed data will NOT be persisted to storage.
                If provided, streamed data will be stored according to the storage config.
            product_specs: Additional product specifications (e.g. strike_price, expiration for options).
                E.g. stream(product='BTC_USDT_OPT', strike_price=10000, expiration='2024-01-01', option_type='CALL').
        '''
        if rollback_period == 'max' and not start_date:
            start_date, end_date, rollback_period = self._handle_rollback_max_period(resolution, start_date, end_date)
        return super().stream(
            product=product,
            resolution=resolution,
            symbol=symbol,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            callback=callback,
            data_origin=data_origin,
            env=env,
            stream_mode=stream_mode,
            flush_interval=flush_interval,
            clean_data=clean_data,
            storage_config=storage_config,
            **product_specs
        )
        
    async def _stream_impl(
        self, 
        faucet_callback: Callable[[WebSocketName, Message, ChannelKey | None], Coroutine[Any, Any, None]]
    ):
        async def _callback(msg: Message):
            symbol: str = msg['id']
            channel_key: ChannelKey = self.stream_api.generate_channel_key(symbol)
            await faucet_callback(self.name.value, msg, channel_key)
        self.stream_api.set_callback(_callback)
        await self.stream_api.connect()
    
    async def _close_stream(self):
        await self.stream_api.disconnect()
    
    def _get_default_transformations_for_stream(self) -> list[Callable[..., Any]]:
        from pfeed.utils import lambda_with_name
        from pfeed.requests import MarketFeedStreamRequest
        request: MarketFeedStreamRequest = cast(MarketFeedStreamRequest, self._current_request)
        default_transformations = MarketFeed._get_default_transformations_for_stream(self)
        # since Ray can't serialize the "self" in self._parse_message, disable it for now
        assert self._num_workers is None, "Transformations in Yahoo Finance streaming data is not supported with Ray"
        if request.clean_data:
            default_transformations = [
                lambda_with_name(
                    'parse_message', 
                    lambda msg: self._parse_message(request.product, msg)  # pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType]
                ),
            ] + default_transformations
        return default_transformations
    
    def _parse_message(self, product: BaseProduct, msg: dict[str, Any]) -> dict[str, Any]:
        '''
        Args:
            msg: raw message from yahoo finance streaming data
        NOTE: lots of quirks in yahoo finance streaming data:
        - weird 'last_size', sometimes it's provided, sometimes not
        - 'time' can be duplicated, i.e. trades can be backfilled, 
        e.g.
            {'id': 'AAPL', 'price': 230.605, 'time': '1755531691000', 'exchange': 'NMS', 'quote_type': 8, 'market_hours': 1, 'change_percent': -0.42533004, 'day_volume': '14805050', 'change': -0.9850006, 'last_size': '120', 'price_hint': '2'}
            {'id': 'AAPL', 'price': 230.605, 'time': '1755531691000', 'exchange': 'NMS', 'quote_type': 8, 'market_hours': 1, 'change_percent': -0.4253209, 'day_volume': '14805114', 'change': -0.9850006, 'price_hint': '2'}
        currently interpret it as this is how yahoo finance handles delayed trades
        REVIEW: current solution is, ignore 'last_size', derive volume = diff(day_volume - previous day_volume); +1 to duplicated 'time'
        another more accurate solution to handle duplicated 'time' is, wait for the 'time' to change to get the most accurate volume, 
        but it might not be worth it due the quality of yahoo finance streaming data
        ''' 
        channel_key: ChannelKey = self.stream_api.generate_channel_key(product.symbol)
        
        # derive traded volume
        last_day_volume = self.stream_api.last_day_volume.get(channel_key, None)
        current_day_volume = int(msg['day_volume'])
        if last_day_volume is not None:
            volume = current_day_volume - last_day_volume
        else:
            volume = None
        self.stream_api.update_last_day_volume(channel_key, current_day_volume)
        
        # detect duplicated 'time', if duplicated, add 1 to it to make it unique
        last_time_in_mts = self.stream_api.last_time_in_mts.get(channel_key, None)
        current_time_in_mts = int(msg['time'])
        if last_time_in_mts is not None and current_time_in_mts == last_time_in_mts:
            current_time_in_mts += 1
        ts = current_time_in_mts / 1000  # convert to seconds
        self.stream_api.update_last_time_in_mts(channel_key, current_time_in_mts)
        
        parsed_msg = {
            'data': {
                'ts': ts,
                'price': msg['price'],
                'volume': volume,
            },
            'extra_data': {
                'exchange': msg['exchange'],
                'market_hours': msg['market_hours'],
                'last_size': msg.get('last_size', None),
            }
        }
        return parsed_msg
    

    ##############################################################################################
    # EXTEND: Functions using yfinance for convenience
    ##############################################################################################
    def get_option_expirations(self, symbol: str) -> tuple[str, ...]:
        '''Get all available option expirations for a given symbol.'''
        ticker: Ticker = self.batch_api.Ticker(symbol)
        expirations: tuple[str, ...] = cast(tuple[str, ...], ticker.options)
        return expirations

    # TODO: standardize the df? e.g. standardize column names, date format etc.
    def get_option_chain(self, symbol: str, expiration: str, option_type: Literal['CALL', 'PUT']) -> GenericFrame:
        '''Get the option chain for a given symbol, expiration, and option type.
        Args:
            expiration: e.g. '2024-12-13', it must be one of the values returned by `get_option_expirations`.
            option_type: 'CALL' or 'PUT'
        '''
        import pandas as pd  # pyright: ignore[reportUnusedImport]
        from pfeed.typing import GenericFrame  # pyright: ignore[reportUnusedImport]
        from pfeed._etl.base import convert_to_desired_df
        from pfeed.config import get_config
        
        config = get_config()
        
        ticker: Ticker = self.batch_api.Ticker(symbol)
        option_chain = ticker.option_chain(expiration)  # pyright: ignore[reportUnknownMemberType]
        if option_type.upper() == 'CALL':
            df: pd.DataFrame = cast(pd.DataFrame, option_chain.calls)
        elif option_type.upper() == 'PUT':
            df: pd.DataFrame = cast(pd.DataFrame, option_chain.puts)
        else:
            raise ValueError(f"Invalid option type: {option_type}")
        return cast(GenericFrame, convert_to_desired_df(df, config.data_tool))
    