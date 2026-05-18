# pyright: reportUnknownMemberType=false, reportAttributeAccessIssue=false, reportUnknownVariableType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Callable, ClassVar, Any, cast, Self
if TYPE_CHECKING:
    from collections.abc import Coroutine
    import pandas as pd
    from yfinance import Ticker
    from narwhals.typing import IntoFrame
    from pfeed.dataflow.result import RunResult
    from pfeed.feeds.streaming_feed_mixin import ParsedMessage, WebSocketName, RawMessage
    from pfund.datas.resolution import Resolution
    from pfund.entities.products.product_base import BaseProduct
    from pfeed.storages.storage_config import StorageConfig
    from pfeed.sources.yahoo_finance.stream_api import ChannelKey

import time
import datetime

import polars as pl

from pfeed.sources.yahoo_finance.mixin import YahooFinanceMixin
from pfeed.sources.yahoo_finance.market_data_model import YahooFinanceMarketDataModel
from pfeed.feeds.market_feed import MarketFeed
from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
from pfeed._io.io_config import IOConfig


__all__ = []


# NOTE: only yfinance's period='max' is used, everything else is converted to start_date and end_date
# i.e. any resampling inside yfinance (interval always ='1x') is not used, it's all done by pfeed
class YahooFinanceMarketFeed(StreamingFeedMixin, YahooFinanceMixin, MarketFeed):
    data_model_class: ClassVar[type[YahooFinanceMarketDataModel]] = YahooFinanceMarketDataModel
    # "Date" is used for daily data and "Datetime" is used for other resolutions in yfinance
    date_columns_in_raw_data: ClassVar[list[str]] = ['Datetime', 'Date']
    SUPPORTS_ROLLBACK_MAX_PERIOD: ClassVar[bool] = True
    # HACK: use '1900-01-01' as the start date for daily data since we don't know the exact start date when rollback_period == 'max'
    DAILY_DATA_ROLLBACK_MAX_START_DATE: ClassVar[str] = '1900-01-01'

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
    def _normalize_raw_data(df: pl.LazyFrame) -> pl.LazyFrame:
        '''Normalize raw yfinance DataFrame into a consistent format.

        Args:
            df: DataFrame after `_standardize_date_column`

        Returns:
            Normalized DataFrame with:
            - Lowercased column names with spaces replaced by underscores
            - 'stock_splits' renamed to 'splits'
            - volume cast to float64
        '''
        RENAMING_COLS = {'stock_splits': 'splits'}
        # convert column names to lowercase and replace spaces with underscores
        df = df.rename({col: col.replace(" ", "_").lower() for col in df.collect_schema().names()})
        df = df.rename(RENAMING_COLS)
        # convert volume (int) to float
        df = df.with_columns(pl.col('volume').cast(pl.Float64))
        return df

    def _rollback_max_period(self, resolution: Resolution) -> tuple[datetime.date | str | None, datetime.date | str | None, str]:
        resolution = Resolution(resolution)
        if resolution.is_day():
            start_date = self.DAILY_DATA_ROLLBACK_MAX_START_DATE
            end_date = None
            rollback_period = 'max'
        elif resolution.is_hour():
            start_date = end_date = None
            rollback_period = '2y'  # max is 2 years for hourly data
        elif resolution.is_minute():
            start_date = end_date = None
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
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        clean_data: bool=True,
        storage_config: StorageConfig | None=None,
        io_config: IOConfig | None=None,
        yfinance_kwargs: dict[str, Any] | None=None,
        **product_specs: Any
    ) -> Self | RunResult:
        '''Download historical data from Yahoo Finance.

        Note: data covering today may be incomplete since the day hasn't closed.
        Affects `rollback_period='ytd'` and `'max'` in particular. To avoid this,
        pass `start_date`/`end_date` explicitly with `end_date` set to a past day.

        Args:
            product: Product basis (e.g. 'AAPL_USD_STK'). For products
                with extra attributes (options, futures), pass them via `product_specs`.
            resolution: Target data resolution (e.g. '1m', '1h', 'day'). If the source
                doesn't provide this resolution natively, finer-grained source data is
                downloaded and resampled down.
            symbol: yfinance Ticker symbol. If empty, derived from `product` — but the
                derivation may be wrong, in which case pass it explicitly.
            rollback_period: Lookback from today, only used when `start_date` is empty.
                Accepts a resolution string (e.g. '7d'), 'ytd', or 'max'. With 'max',
                yfinance-specific lookback caps apply per resolution (see
                `_rollback_max_period`).
            start_date: Start date. If empty, derived from `rollback_period`.
            end_date: End date. If empty, defaults to today.
            clean_data: Whether to clean raw data after download.
                Ignored when `storage_config` is provided — cleaning is then determined
                by `data_layer`. If True, runs default transformations (normalize,
                standardize columns, resample). If False, raw data is returned as-is.
            storage_config: Where to persist downloaded data. If None, data is not
                persisted to storage.
            io_config: IO format/compression and read/write/connect options. Defaults
                to parquet + snappy.
            yfinance_kwargs: Extra kwargs forwarded directly to yfinance's
                `Ticker.history()`. See `history()` in `yfinance/scrapers/history.py`.
                `interval`, `period`, `start`, `end` are managed by pfeed and must not
                be passed here.
            product_specs: Extra product attributes for products that need them, e.g.
                `download(product='AAPL_USD_OPT', strike_price=150,
                expiration='2024-01-01', option_type='CALL')`. Leave empty first and
                read the exception message to discover required keys.

        Returns:
            RunResult of the download operation.
            Returns `self` when called in pipeline mode.
        '''
        self._yfinance_kwargs: dict[str, Any] = self._check_yfinance_kwargs(yfinance_kwargs)
        return super().download(
            product=product,
            symbol=symbol,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            dataflow_per_date=False,
            storage_config=storage_config,
            io_config=io_config,
            clean_data=clean_data,
            **product_specs
        )

    def _download_impl(self, data_model: YahooFinanceMarketDataModel, data_resolution: Resolution) -> pl.LazyFrame | None:
        # convert pfund's resolution format to yfinance's interval
        timeframe = repr(data_resolution.timeframe)
        etimeframe = self._ADAPTER["timeframe"].get(timeframe, timeframe)  # external timeframe
        eresolution = str(data_resolution.period) + etimeframe  # external resolution

        product = data_model.product
        symbol = product.symbol
        assert symbol, f'symbol is required for {product}'
        start_date, end_date = data_model.start_date, data_model.end_date

        batch_api = self.data_source.get_batch_api()
        ticker = batch_api.Ticker(symbol)

        df: pd.DataFrame | None = None
        num_retries = 3
        # NOTE: yfinance's end_date is not inclusive, so we need to add 1 day to the end_date
        yfinance_end_date = end_date + datetime.timedelta(days=1)

        while num_retries:
            num_retries -= 1
            self.logger.debug(f'downloading {product} {data_resolution} from {start_date} to {end_date}')
            # NOTE: yfinance's period is not used, only use start_date and end_date for data clarity in storage
            df: pd.DataFrame | None = ticker.history(
                interval=eresolution,
                start=str(start_date),
                end=str(yfinance_end_date),
                **self._yfinance_kwargs
            )
            if df is None or df.empty:
                if num_retries:
                    self.logger.info(f'failed to download {symbol} {data_resolution} data, retrying...')
                    time.sleep(1)
            else:
                # convert tz-aware index to UTC naive, then reset to column
                if hasattr(df.index, 'tz') and df.index.tz is not None:
                    df.index = df.index.tz_convert("UTC").tz_localize(None)
                if str(start_date) == self.DAILY_DATA_ROLLBACK_MAX_START_DATE:
                    start_date = min(df.index).date()
                    self.logger.debug(f'got actual start date, set start_date={start_date}')
                # NOTE: reset index here so date columns (e.g. 'Datetime', 'Date') are actual columns,
                # which is required by _standardize_date_column to find and rename them
                df = df.reset_index()
                break
        else:
            self.logger.warning(
                f'failed to download {symbol} {data_resolution} data after all retries, ' +
                f'please check if start_date={start_date} and end_date={end_date} is within a valid range. ' +
                'If it happens on trading days, it is possibly due to rate limit, network issue, or bugs in `pfeed` or `yfinance`, ' +
                'you may try to extend the time range or avoid using `rollback_period` to try again.'
            )
        self._yfinance_kwargs.clear()
        return pl.from_pandas(df).lazy() if df is not None else None

    async def _stream_impl(
        self,
        faucet_streaming_callback: Callable[[WebSocketName, RawMessage, ChannelKey | None], Coroutine[Any, Any, None]]
    ):
        stream_api = self.data_source.get_stream_api()
        async def _callback(msg: RawMessage):
            symbol: str = msg['id']
            channel_key: ChannelKey = stream_api.generate_channel_key(symbol)
            ws_name = self.name.value
            await faucet_streaming_callback(ws_name, msg, channel_key)
        stream_api.set_callback(_callback)
        await stream_api.connect()

    @staticmethod
    def _parse_message(product: BaseProduct, msg: RawMessage) -> ParsedMessage:
        '''
        Args:
            msg: raw message from yahoo finance streaming data
        NOTE: lots of quirks in yahoo finance streaming data:
        - weird 'last_size', sometimes it's provided, sometimes not
        - 'time' can be duplicated, i.e. trades can be backfilled,
        e.g.
            {'id': 'AAPL', 'price': 230.605, 'time': '1755531691000', 'exchange': 'NMS', 'quote_type': 8, 'market_hours': 1, 'change_percent': -0.42533004, 'day_volume': '14805050', 'change': -0.9850006, 'last_size': '120', 'price_hint': '2'}
            {'id': 'AAPL', 'price': 230.605, 'time': '1755531691000', 'exchange': 'NMS', 'quote_type': 8, 'market_hours': 1, 'change_percent': -0.4253209, 'day_volume': '14805114', 'change': -0.9850006, 'price_hint': '2'}
            # REVIEW: sometimes it doesn't have 'day_volume', not sure why, could be a bug in yfinance?
            {'id': 'AAPL', 'price': 249.8712, 'time': '1774047121000', 'exchange': 'NMS', 'quote_type': 8, 'market_hours': 2, 'change_percent': 0.758577, 'change': 1.8811951, 'price_hint': '2'}
        '''

        # DEPRECATED: give up handling yahoo finance volume, definition of 'day_volume' is not clear
        # currently interpret it as this is how yahoo finance handles delayed trades
        # REVIEW: current solution is, ignore 'last_size', derive volume = diff(day_volume - previous day_volume); +1 to duplicated 'time'
        # another more accurate solution to handle duplicated 'time' is, wait for the 'time' to change to get the most accurate volume,
        # but it might not be worth it due the quality of yahoo finance streaming data

        # stream_api = self.data_source.get_stream_api()
        # channel_key: ChannelKey = stream_api.generate_channel_key(product.symbol)

        # # derive traded volume
        # last_day_volume = stream_api.last_day_volume.get(channel_key, None)
        # # it could be missing for the after-hours messages (market_hours=2)
        # current_day_volume = msg.get('day_volume', None)
        # current_day_volume = int(current_day_volume) if current_day_volume is not None else None
        # if last_day_volume is not None and current_day_volume is not None:
        #     volume = current_day_volume - last_day_volume
        # else:
        #     volume = None
        # if current_day_volume is not None:
        #     stream_api.update_last_day_volume(channel_key, current_day_volume)

        ts = msg.get('time', None)
        if ts is not None:
            ts = int(ts) / 1000  # convert to seconds

        day_volume = msg.get('day_volume', None)
        if day_volume is not None:
            day_volume = float(day_volume)

        # DEPRECATED: tick message now has an "index" for uniqueness, no need to manually make the ts unique
        # detect duplicated 'time', if duplicated, add 1 to it to make it unique
        # last_time_in_mts = self.stream_api.last_time_in_mts.get(channel_key, None)
        # current_time_in_mts = int(msg['time'])
        # if last_time_in_mts is not None and current_time_in_mts == last_time_in_mts:
        #     current_time_in_mts += 1
        # ts = current_time_in_mts / 1000  # convert to seconds
        # self.stream_api.update_last_time_in_mts(channel_key, current_time_in_mts)

        parsed_msg: ParsedMessage = {
            'ts': cast(float, ts),
            'channel': product.symbol,
            'data': {
                'ts': ts,
                'price': msg['price'],
                'volume': day_volume,
                'extra_data': {
                    'exchange': msg.get('exchange', None),
                    'market_hours': msg.get('market_hours', None),
                    'last_size': msg.get('last_size', None),
                }
            },
        }
        return parsed_msg


    ##############################################################################################
    # EXTEND: Functions using yfinance for convenience
    ##############################################################################################
    def get_option_expirations(self, symbol: str) -> tuple[str, ...]:
        '''Get all available option expirations for a given symbol.'''
        batch_api = self.data_source.get_batch_api()
        ticker: Ticker = batch_api.Ticker(symbol)
        expirations: tuple[str, ...] = cast(tuple[str, ...], ticker.options)
        return expirations

    # TODO: standardize the df? e.g. standardize column names, date format etc.
    def get_option_chain(self, symbol: str, expiration: str, option_type: Literal['CALL', 'PUT']) -> IntoFrame:
        '''Get the option chain for a given symbol, expiration, and option type.
        Args:
            expiration: e.g. '2024-12-13', it must be one of the values returned by `get_option_expirations`.
            option_type: 'CALL' or 'PUT'
        '''
        from pfeed._etl.base import convert_dataframe

        batch_api = self.data_source.get_batch_api()
        ticker: Ticker = batch_api.Ticker(symbol)
        option_chain = ticker.option_chain(expiration)
        if option_type.upper() == 'CALL':
            df: pd.DataFrame = option_chain.calls
        elif option_type.upper() == 'PUT':
            df: pd.DataFrame = option_chain.puts
        else:
            raise ValueError(f"Invalid option type: {option_type}")
        return convert_dataframe(df)
