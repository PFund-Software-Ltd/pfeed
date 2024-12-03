from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfeed.types.literals import tSTORAGE
    from pfeed.types.core import tDataFrame
    try:
        import pandas as pd
    except ImportError:
        pass

import datetime

import yfinance
import pandas as pd

from pfeed import etl
from pfeed.feeds.market_data_feed import MarketDataFeed
from pfeed.const.enums import DataRawLevel


__all__ = ["YahooFinanceFeed"]


class YahooFinanceFeed(MarketDataFeed):
    _URLS = {
        "rest": "https://query1.finance.yahoo.com",
        "ws": "wss://streamer.finance.yahoo.com",
    }
    _ADAPTER = {
        "timeframe": {
            "M": "mo",
            "mo": "M",
            "w": "wk",
            "wk": "w",
        }
    }
    # yfinance's valid periods = pfund's rollback_periods
    SUPPORTED_PERIODS = {
        "d": [1, 5],
        "M": [1, 3, 6],
        "y": [1, 2, 5, 10],
    }
    # yfinance's valid intervals = pfund's resolutions
    SUPPORTED_TIMEFRAMES_AND_PERIODS = {
        "m": [1, 2, 5, 15, 30, 60, 90],
        "h": [1],
        "d": [1, 5],
        "w": [1],
        "M": [1, 3],
    }

    @staticmethod
    def get_data_source():
        from pfeed.sources.yahoo_finance.data_source import YahooFinanceDataSource
        return YahooFinanceDataSource()

    def _normalize_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        # convert to UTC and reset index
        df.index = df.index.tz_convert("UTC").tz_localize(None)
        df.reset_index(inplace=True)
        
        # convert column names to lowercase and replace spaces with underscores
        # NOTE:
        # if there are spaces in column names, they will be turned into some weird names like "_10" during "for row in df.itertuples()"
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]

        # somehow different column names "Date" and "Datetime" are used in yfinance depending on the resolution
        RENAMING_COLS = {'date': 'ts', 'datetime': 'ts', 'stock_splits': 'splits'}
        df = df.rename(columns=RENAMING_COLS)
        return df
   
    def get_option_expirations(self, symbol: str) -> tuple[str]:
        '''Get all available option expirations for a given symbol.'''
        ticker: yfinance.Ticker = self.api.Ticker(symbol)
        expirations = ticker.options
        return expirations

    # TODO: standardize the df? e.g. standardize column names, date format etc.
    def get_option_chain(self, symbol: str, expiration: str, option_type: Literal['CALL', 'PUT']) -> tDataFrame:
        '''Get the option chain for a given symbol, expiration, and option type.
        Args:
            expiration: e.g. '2024-12-13', it must be one of the values returned by `get_option_expirations`.
            option_type: 'CALL' or 'PUT'
        '''
        ticker: yfinance.Ticker = self.api.Ticker(symbol)
        option_chain = ticker.option_chain(expiration)
        if option_type.upper() == 'CALL':
            df = option_chain.calls
        elif option_type.upper() == 'PUT':
            df = option_chain.puts
        else:
            raise ValueError(f"Invalid option type: {option_type}")
        return etl.convert_to_user_df(df, self.data_tool.name)

    def get_historical_data(
        self,
        symbol: str,
        resolution: str = "1d",
        rollback_period: str | Literal["ytd", "max"] = "1M",
        start_date: str = "",
        end_date: str = "",
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        use_pfeed_resample: bool = True,
        **kwargs,
    ) -> tDataFrame | None:
        """Simple Wrapper of yfinance history().
        For the details of args and kwargs, please refer to https://github.com/ranaroussi/yfinance

        Args:
            symbol: yfinance's symbol, e.g. AAPL, TSLA
            rollback_period: Data resolution or 'ytd' or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1M' = 1 month.
                if 'period' in kwargs is specified, it will be used instead of `rollback_period`.
            resolution: Data resolution
                e.g. '1m' = 1 minute as the unit of each data bar/candle.
                Default is '1d' = 1 day.
            raw_level:
                'cleaned' (least raw): normalize data (refer to 'normalized' below), also remove all non-standard columns
                    e.g. standard columns in stock data are ts, product, open, high, low, close, volume, dividends, splits
                'normalized' (default): perform normalization following pfund's convention, preserve all columns
                    Normalization example:
                    - renaming: 'timestamp' -> 'ts'
                    - mapping: 'buy' -> 1, 'sell' -> -1
                'original' (most raw): keep the original data from yfinance, no transformation will be performed.
                It will be ignored if the data is loaded from storage but not downloaded.
            # REVIEW: yfinance's resampling result looks incorrect, e.g. try using '5d' resolution, ohlc is wrong
            use_pfeed_resample: Whether to use pfeed's resampling logic.
                Default is True.
                This will automatically be triggered if yfinance does not support the resolution.
            **kwargs: kwargs supported by `yfinance`
        """
        from pfund.datas.resolution import Resolution

        def _get_yfinance_period(rollback_period: str | Literal["ytd", "max"], kwargs: dict) -> str:
            if rollback_period in ["ytd", "max"]:
                period = rollback_period
            else:
                # if user is directly using yfinance variable `period`, use it
                if "period" in kwargs:
                    period = kwargs["period"]
                    del kwargs["period"]
                else:
                    # convert pfund's rollback_period format to yfinance's period
                    rollback_period = Resolution(rollback_period)
                    timeframe = repr(rollback_period.timeframe)
                    etimeframe = self._ADAPTER["timeframe"].get(timeframe, timeframe)
                    erollback_period = str(rollback_period.period) + etimeframe
                    period = erollback_period
            return period
        
        def _get_yfinance_interval_and_pfund_resolution_and_timeframe(resolution: str, kwargs: dict) -> tuple[str, Resolution, str, str]:
            from pfeed.utils.utils import separate_number_and_chars
            # if user is directly using yfinance variable `interval`, use it
            if "interval" in kwargs:
                interval = kwargs["interval"]
                del kwargs["interval"]
                number_part, char_part = separate_number_and_chars(interval)
                etimeframe = char_part
                timeframe = self._ADAPTER["timeframe"].get(etimeframe, etimeframe)
                resolution = Resolution(number_part + timeframe)
            else:
                # convert pfund's resolution format to yfinance's interval
                resolution = Resolution(resolution)
                timeframe = repr(resolution.timeframe)
                etimeframe = self._ADAPTER["timeframe"].get(timeframe, timeframe)
                interval = str(resolution.period) + etimeframe    
            return interval, resolution, timeframe, etimeframe
        
        def _adjust_interval_if_not_supported(interval: str, resolution: Resolution, timeframe: str, etimeframe: str) -> tuple[str, bool]:
            # manipulate the input resolution and support e.g. '2d' resolution even it is not in the SUPPORTED_TIMEFRAMES_AND_PERIODS
            is_period_not_supported = (
                timeframe in self.SUPPORTED_TIMEFRAMES_AND_PERIODS
                and resolution.period not in self.SUPPORTED_TIMEFRAMES_AND_PERIODS[timeframe]
                and 1 in self.SUPPORTED_TIMEFRAMES_AND_PERIODS[timeframe]
            )
            if (use_pfeed_resample and resolution.period != 1) or is_period_not_supported:
                # if resolution (e.g. '2d') is not supported in yfinance, using "1d" instead'
                interval = "1" + etimeframe
                is_resample = True
            else:
                is_resample = False
            return interval, is_resample
        
        def _get_yfinance_start_and_end_date(start_date: str, end_date: str) -> tuple[str, str]:
            if start_date:
                start_date, end_date = self._standardize_dates(start_date, end_date)
                # if start_date and end_date are the same, yfinance will return an empty dataframe
                if start_date == end_date:
                    end_date += datetime.timedelta(days=1)
                start_date, end_date = str(start_date), str(end_date)
            else:
                start_date = end_date = None
            return start_date, end_date
            
        raw_level = DataRawLevel[raw_level.upper()]
        period = _get_yfinance_period(rollback_period, kwargs)
        interval, resolution, timeframe, etimeframe = _get_yfinance_interval_and_pfund_resolution_and_timeframe(resolution, kwargs)
        interval, is_resample = _adjust_interval_if_not_supported(interval, resolution, timeframe, etimeframe)
        start_date, end_date = _get_yfinance_start_and_end_date(start_date, end_date)

        ticker = self.api.Ticker(symbol)
        df = ticker.history(period=period, interval=interval, start=start_date, end=end_date, **kwargs)
        if isinstance(df, pd.DataFrame):
            if df.empty:
                return None
            if raw_level != DataRawLevel.ORIGINAL:
                df = self._normalize_raw_data(df)
                df = etl.organize_columns(df, resolution, symbol=symbol)
                # only resample if raw_level is 'cleaned', otherwise, can't resample non-standard columns
                if raw_level == DataRawLevel.CLEANED:
                    df = etl.filter_non_standard_columns(df)
                    if is_resample:
                        if not use_pfeed_resample:
                            self.logger.warning(
                                f"yfinance does not support {repr(resolution)} as an interval, auto-resampling to {resolution=}"
                            )
                        df = etl.resample_data(df, resolution)
            
            return etl.convert_to_user_df(df, self.data_tool.name)
        else:
            raise ValueError(f"yfinance returned {type(df)} instead of pd.DataFrame")
