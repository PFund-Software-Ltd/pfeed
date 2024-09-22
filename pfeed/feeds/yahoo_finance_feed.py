from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS
    try:
        import pandas as pd
    except ImportError:
        pass

import datetime

try:
    import polars as pl
except ImportError:
    pass

from pfeed.feeds.base_feed import BaseFeed
from pfeed.utils.utils import separate_number_and_chars


__all__ = ["YahooFinanceFeed"]


class YahooFinanceFeed(BaseFeed):
    _ADAPTER = {
        "timeframe": {
            "M": "mo",
            "mo": "M",
            "w": "wk",
            "wk": "w",
        }
    }
    # yfinance's valid periods
    SUPPORTED_PERIODS = {
        "d": [1, 5],
        "M": [1, 3, 6],
        "y": [1, 2, 5, 10],
    }
    # yfinance's valid intervals
    SUPPORTED_TIMEFRAMES_AND_PERIODS = {
        "m": [1, 2, 5, 15, 30, 60, 90],
        "h": [1],
        "d": [1, 5],
        "w": [1],
        "M": [1, 3],
    }

    def __init__(self, data_tool: tSUPPORTED_DATA_TOOLS = "pandas"):
        import yfinance
        super().__init__("yahoo_finance", data_tool=data_tool)
        self.api = yfinance

    def get_ticker(self, symbol):
        return self.api.Ticker(symbol.upper())

    def get_historical_data(
        self,
        symbol: str,
        resolution: str = "1d",
        rollback_period: str | Literal["ytd", "max"] = "1M",
        start_date: str = "",
        end_date: str = "",
        use_pfeed_resample: bool = True,
        product: str = "",
        **kwargs,
    ) -> pd.DataFrame | pl.DataFrame:
        """Simple Wrapper of yfinance history().
        For the details of args and kwargs, please refer to https://github.com/ranaroussi/yfinance

        Args:
            symbol: ticker symbol used in yfinance
            rollback_period: Data resolution or 'ytd' or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1M' = 1 month.
                if 'period' in kwargs is specified, it will be used instead of `rollback_period`.
            resolution: Data resolution
                e.g. '1m' = 1 minute as the unit of each data bar/candle.
                Default is '1d' = 1 day.
            # REVIEW: yfinance's resampling result looks incorrect, e.g. try using '5d' resolution, ohlc is wrong
            use_pfeed_resample: Whether to use pfeed's resampling logic.
                Default is True.
                This will automatically be triggered if yfinance does not support the resolution.
            product: Product symbol, e.g. AAPL_USD_STK. If provided, it will be used to create a column 'product' in the output dataframe.
            **kwargs: kwargs supported by `yfinance`
        """
        from pfeed import etl
        from pfund.datas.resolution import Resolution

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

        # manipulate the input resolution and support e.g. '2d' resolution even it is not in the SUPPORTED_TIMEFRAMES_AND_PERIODS
        if (use_pfeed_resample and resolution.period != 1) or (
            timeframe in self.SUPPORTED_TIMEFRAMES_AND_PERIODS
            and resolution.period not in self.SUPPORTED_TIMEFRAMES_AND_PERIODS[timeframe]
            and 1 in self.SUPPORTED_TIMEFRAMES_AND_PERIODS[timeframe]
        ):
            # if resolution (e.g. '2d') is not supported in yfinance, using "1d" instead'
            interval = "1" + etimeframe
            is_resample = True
        else:
            is_resample = False

        if start_date:
            # default for end_date is today
            today = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y-%m-%d")
            end_date = end_date or today
            assert (
                start_date != end_date
            ), f"{start_date=} and {end_date=} cannot be the same, end_date in yfinance is exclusive"
        else:
            start_date = end_date = None

        ticker = self.get_ticker(symbol)
        df = ticker.history(
            period=period, interval=interval, start=start_date, end=end_date, **kwargs
        )

        assert not df.empty, f"No data found for {symbol=}, {resolution=}, {rollback_period=}, {start_date=}, {end_date=}."

        # convert to UTC
        df.index = df.index.tz_convert("UTC")
        # convert to UTC and remove +hh:mm from YYYY-MM-DD hh:mm:ss+hh:mm
        df.index = df.index.tz_convert("UTC").tz_localize(None)
        df.reset_index(inplace=True)
        df.columns = df.columns.str.lower()
        # if there are spaces in column names, they will be turned into some weird names like "_10"
        # during "for row in df.itertuples()"
        # Replace spaces in the column names with underscores
        df.columns = [col.replace(" ", "_") for col in df.columns]
        # somehow different column names "Date" and "Datetime" are used in yfinance depending on the resolution
        df = df.rename(columns={
            "date": "ts", 
            "datetime": "ts",
            'stock_splits': 'splits',
        })

        if is_resample:
            if not use_pfeed_resample:
                self.logger.warning(
                    f"yfinance does not support {repr(resolution)} as an interval, auto-resampling to {resolution=}"
                )
            df = etl.resample_data(df, resolution)
        
        df["symbol"] = symbol
        if product:
            df["product"] = product
        df["resolution"] = repr(resolution)
        # reorder columns
        if "product" in df.columns:
            left_cols = ["ts", "symbol", "product", "resolution"]
        else:
            left_cols = ["ts", "symbol", "resolution"]
        df = df[left_cols + [col for col in df.columns if col not in left_cols]]
        
        if self.data_tool.name == "pandas":
            return df
        elif self.data_tool.name == "polars":
            return pl.from_pandas(df)
