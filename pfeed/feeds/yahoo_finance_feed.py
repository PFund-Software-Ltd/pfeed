from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from pfeed.types.literals import tSTORAGE
    from pfeed.types.core import tDataModel
    from pfeed.types.core import tDataFrame
    from pfeed.flows.dataflow import DataFlow
    from pfeed.const.enums import DataRawLevel

import time
import datetime

import yfinance
import pandas as pd

from pfund.datas.resolution import Resolution
from pfeed import etl
from pfeed.feeds.base_feed import clear_current_dataflows
from pfeed.feeds.market_data_feed import MarketDataFeed


__all__ = ["YahooFinanceFeed"]


# NOTE: only yfinance's period='max' is used, everything else is converted to start_date and end_date
# i.e. any resampling inside yfinance (interval always ='1x') is not used, it's all done by pfeed
class YahooFinanceFeed(MarketDataFeed):
    _URLS = {
        "rest": "https://query1.finance.yahoo.com",
        "ws": "wss://streamer.finance.yahoo.com",
    }
    _ADAPTER = {
        "timeframe": {
            # month
            "M": "mo",
            "mo": "M",
            # week
            "w": "wk",
            "wk": "w",
        }
    }
    # yfinance's valid periods = pfund's rollback_periods
    SUPPORTED_ROLLBACK_PERIODS = {
        "d": [1, 5],
        "M": [1, 3, 6],
        "y": [1, 2, 5, 10],
    }
    # yfinance's valid intervals = pfund's resolutions
    SUPPORTED_RESOLUTIONS = {
        "m": [1, 2, 5, 15, 30, 60, 90],
        "h": [1],
        "d": [1, 5],
        "w": [1],
        "M": [1, 3],
    }
    
    _yfinance_kwargs: dict | None = None

    @staticmethod
    def get_data_source():
        from pfeed.sources.yahoo_finance.source import YahooFinanceSource
        return YahooFinanceSource()
    
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
    
    def _prepare_yfinance_kwargs(self, yfinance_kwargs: dict | None) -> dict:
        if self._yfinance_kwargs is not None:
            return self._yfinance_kwargs
        yfinance_kwargs = yfinance_kwargs or {}
        assert "interval" not in yfinance_kwargs, "`interval` duplicates with `resolution`, please remove it"
        assert "period" not in yfinance_kwargs, "`period` duplicates with `rollback_period`, please remove it"
        return yfinance_kwargs
    
    # TODO
    @clear_current_dataflows
    def stream(self) -> YahooFinanceFeed:
        raise NotImplementedError(f'{self.name} stream() is not implemented')
        return self
    
    # TODO
    def _execute_stream(self, data_model: tDataModel):
        raise NotImplementedError(f'{self.name} _execute_stream() is not implemented')
    
    def download(
        self,
        products: str | list[str] | None=None,
        symbols: str | list[str] | None=None,
        data_type: Literal['minute', 'hour', 'day']='day',
        rollback_period: str | Literal["ytd", "max"]='max',
        start_date: str='',
        end_date: str='',
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        to_storage: tSTORAGE='local',
        filename_prefix: str='',
        filename_suffix: str='',
        product_specs: dict[str, dict] | None=None,  # {'product_basis': {'attr': 'value', ...}}
        yfinance_kwargs: dict | None=None,
    ) -> YahooFinanceFeed:
        '''
        Download historical data from Yahoo Finance.
        Be reminded that if you include today's data, it can be incomplete, this especially applies to the usage of rollback_period.
        e.g. rollback_period='ytd'/'max' includes today's data, and today is not finished, so the data is incomplete.
        start_date and end_date (not including today) should be specified to avoid this issue.
        
        Args:
            filename_prefix: The prefix of the filename.
            filename_suffix: The suffix of the filename.
            product_specs: The specifications for the products.
                'TSLA_USD_OPT' is in `products`, you need to provide the specifications of the option in `product_specs`:
                e.g. {'TSLA_USD_OPT': {'strike_price': 500, 'expiration': '2024-01-01', 'option_type': 'CALL'}}
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
            rollback_period: Data resolution or 'ytd' or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1M' = 1 month.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use yesterday's date as the default start date.
            end_date: End date.
                If not specified, use today's date as the end date.
            yfinance_kwargs: kwargs supported by `yfinance`
                refer to kwargs in history() in yfinance/scrapers/history.py
        '''
        self._yfinance_kwargs = self._prepare_yfinance_kwargs(yfinance_kwargs)
        data_type = data_type.lower()
        # makes rollback_period == 'max' more specific for different data types
        if rollback_period == 'max':
            if data_type == 'hour':
                rollback_period = '2y'  # max is 2 years for hourly data
            elif data_type == 'minute':
                rollback_period = '8d'  # max is 8 days for minute data
        return super().download(
            products=products,
            symbols=symbols,
            product_types=None,
            data_type=data_type,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            raw_level=raw_level,
            to_storage=to_storage,
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
            product_specs=product_specs,
        )
    
    def _create_download_dataflows(
        self,
        product: BaseProduct,
        resolution: Resolution,
        raw_level: DataRawLevel,
        start_date: datetime.date,
        end_date: datetime.date,
        filename_prefix: str,
        filename_suffix: str,
    ) -> list[DataFlow]:
        data_model = self.create_market_data_model(
            product,
            resolution,
            raw_level,
            start_date=start_date,
            end_date=end_date,
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
        )
        # create a dataflow that schedules _execute_download()
        dataflow: DataFlow = super().extract('download', data_model)
        return [dataflow]

    def _execute_download(self, data_model: tDataModel) -> pd.DataFrame | None:
        # convert pfund's resolution format to yfinance's interval
        resolution = data_model.resolution
        timeframe = repr(resolution.timeframe)
        assert timeframe in self.SUPPORTED_RESOLUTIONS, f'timeframe={resolution.timeframe} is not supported'
        etimeframe = self._ADAPTER["timeframe"].get(timeframe, timeframe)
        eresolution = str(resolution.period) + etimeframe
        
        symbol = data_model.product.symbol
        ticker = self.api.Ticker(symbol)
        
        df = None
        num_retries = 5
        original_start_date = data_model.start_date
        while df is None and num_retries:
            num_retries -= 1
            # NOTE: yfinance's period is not used, only use start_date and end_date for data clarity in storage
            df: pd.DataFrame | None = ticker.history(
                interval=eresolution,
                start=str(data_model.start_date),
                end=str(data_model.end_date),
                **self._yfinance_kwargs
            )
            # for some unknown reason, yfinance sometimes returns None even start_date and end_date are within the valid range
            # so we need to increment start_date by 1 day to shorten the date range and try again
            if df is None:
                closer_start_date = data_model.start_date + datetime.timedelta(days=1)
                data_model.update_start_date(closer_start_date)
                if data_model.start_date >= data_model.end_date:
                    break
                time.sleep(0.1)
            else:
                break
        else:
            self.logger.warning(f'Failed to download {data_model.product} {data_model.resolution} data, '
                                f'please check if start_date={original_start_date} and end_date={data_model.end_date} is within the valid range')
        self._yfinance_kwargs.clear()
        # TODO: better handling of rate limit control
        time.sleep(1)
        return df

    def get_historical_data(
        self,
        product: str,
        symbol: str = '',
        resolution: str = "1d",
        rollback_period: str | Literal["ytd", "max"] = "max",
        start_date: str = "",
        end_date: str = "",
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        from_storage: tSTORAGE | None=None,
        unique_identifier: str='',
        yfinance_kwargs: dict | None=None,
        **product_specs,
    ) -> tDataFrame | None:
        """Gets historical data from Yahoo Finance using yfinance's Ticker.history().
        Args:
            product: product basis, e.g. AAPL_USD_STK, BTC_USDT_PERP
            symbol: yfinance's symbol, e.g. AAPL, TSLA
                if not provided, the first part of `product` split by '_' will be used.
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
            yfinance_kwargs: kwargs supported by `yfinance`
                refer to kwargs in history() in yfinance/scrapers/history.py
        """
        self._yfinance_kwargs = self._prepare_yfinance_kwargs(yfinance_kwargs)
        df = super().get_historical_data(
            product,
            symbol=symbol,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            raw_level=raw_level,
            from_storage=from_storage,
            unique_identifier=unique_identifier,
            **product_specs,
        )
        self._yfinance_kwargs.clear()
        return df


    ###
    # Functions using yfinance for convenience
    ###
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