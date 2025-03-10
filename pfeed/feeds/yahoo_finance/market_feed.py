from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import pandas as pd
    from yfinance import Ticker
    from pfund.products.product_base import BaseProduct
    from pfund.datas.resolution import Resolution
    from pfeed.typing.literals import tSTORAGE, tDATA_LAYER
    from pfeed.typing.core import tDataFrame
    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.flows.dataflow import DataFlow

import time
import datetime

from pfeed.feeds.market_feed import MarketFeed


__all__ = ["YahooFinanceMarketFeed"]


# NOTE: only yfinance's period='max' is used, everything else is converted to start_date and end_date
# i.e. any resampling inside yfinance (interval always ='1x') is not used, it's all done by pfeed
class YahooFinanceMarketFeed(MarketFeed):
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
    # SUPPORTED_ROLLBACK_PERIODS = {
    #     "d": [1, 5],
    #     "M": [1, 3, 6],
    #     "y": [1, 2, 5, 10],
    # }
    
    # yfinance's valid intervals = pfund's resolutions
    # SUPPORTED_RESOLUTIONS = {
    #     "m": [1, 2, 5, 15, 30, 60, 90],
    #     "h": [1],
    #     "d": [1, 5],
    #     "w": [1],
    #     "M": [1, 3],
    # }
    
    _yfinance_kwargs: dict | None = None

    @staticmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        # convert to UTC and reset index
        df.index = df.index.tz_convert("UTC").tz_localize(None)
        df.reset_index(inplace=True)
        
        # convert column names to lowercase and replace spaces with underscores
        # NOTE:
        # if there are spaces in column names, they will be turned into some weird names like "_10" during "for row in df.itertuples()"
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]

        # convert volume (int) to float
        df['volume'] = df['volume'].astype(float)
        
        # somehow different column names "Date" and "Datetime" are used in yfinance depending on the resolution
        RENAMING_COLS = {'datetime': 'date', 'stock_splits': 'splits'}
        df = df.rename(columns=RENAMING_COLS)
        return df
    
    def _check_yfinance_kwargs(self, yfinance_kwargs: dict | None) -> dict:
        if self._yfinance_kwargs is not None:
            return self._yfinance_kwargs
        yfinance_kwargs = yfinance_kwargs or {}
        assert "interval" not in yfinance_kwargs, "`interval` duplicates with pfeed's `resolution`, please remove it"
        assert "period" not in yfinance_kwargs, "`period` duplicates with pfeed's `rollback_period`, please remove it"
        assert "start" not in yfinance_kwargs, "`start` duplicates with pfeed's `start_date`, please remove it"
        assert "end" not in yfinance_kwargs, "`end` duplicates with pfeed's `end_date`, please remove it"
        return yfinance_kwargs
    
    # TODO
    def stream(self) -> YahooFinanceMarketFeed:
        raise NotImplementedError(f'{self.name} stream() is not implemented')
        return self

    # TODO
    def _execute_stream(self, data_model: MarketDataModel):
        raise NotImplementedError(f'{self.name} _execute_stream() is not implemented')
    
    def download(
        self,
        product: str,
        symbol: str='',
        resolution: Resolution | str | Literal['minute', 'hour', 'day']='day',
        rollback_period: str | Literal["ytd", "max"]='max',
        start_date: str='',
        end_date: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        to_storage: tSTORAGE='local',
        storage_configs: dict | None=None,
        auto_transform: bool=True,
        concat_output: bool=True,
        yfinance_kwargs: dict | None=None,
        **product_specs
    ) -> tDataFrame | None | dict[datetime.date, tDataFrame | None] | YahooFinanceMarketFeed:
        '''
        Download historical data from Yahoo Finance.
        Be reminded that if you include today's data, it can be incomplete, this especially applies to the usage of rollback_period.
        e.g. rollback_period='ytd'/'max' includes today's data, and today is not finished, so the data is incomplete.
        start_date and end_date (not including today) should be specified to avoid this issue.
        
        Args:
            product_specs: The specifications for the product.
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
            concat_output: Whether to concatenate the data from different dates.
                If True, the data from different dates will be concatenated into a single DataFrame.
                If False, the data from different dates will be returned as a dictionary of DataFrames with date as the key.
            yfinance_kwargs: kwargs supported by `yfinance`
                refer to kwargs in history() in yfinance/scrapers/history.py
        '''
        from pfeed.const.enums import MarketDataType

        self._yfinance_kwargs = self._check_yfinance_kwargs(yfinance_kwargs)
        resolution: Resolution = self.create_resolution(resolution)
        # makes rollback_period == 'max' more specific for different data types
        if rollback_period == 'max':
            dtype = MarketDataType[str(resolution.timeframe)]
            if dtype == MarketDataType.HOUR:
                rollback_period = '2y'  # max is 2 years for hourly data
            elif dtype == MarketDataType.MINUTE:
                rollback_period = '8d'  # max is 8 days for minute data
        return super().download(
            product=product,
            symbol=symbol,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            to_storage=to_storage,
            storage_configs=storage_configs,
            auto_transform=auto_transform,
            concat_output=concat_output,
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
        # NOTE: one data model for the entire date range
        data_model = self.create_data_model(
            product,
            unit_resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
        )
        # create a dataflow that schedules _execute_download()
        dataflow = self._extract_download(data_model)
        return [dataflow]

    def _execute_download(self, data_model: MarketDataModel) -> pd.DataFrame | None:
        # convert pfund's resolution format to yfinance's interval
        resolution = data_model.resolution
        timeframe = repr(resolution.timeframe)
        etimeframe = self._ADAPTER["timeframe"].get(timeframe, timeframe)
        eresolution = str(resolution.period) + etimeframe
        
        symbol = data_model.product.symbol
        assert symbol, f'symbol is required for {data_model}'
        ticker = self.api.Ticker(symbol)
        
        df = None
        num_retries = 5
        original_start_date = data_model.start_date
        # NOTE: yfinance's end_date is not inclusive, so we need to add 1 day to the end_date
        yfinance_end_date = data_model.end_date + datetime.timedelta(days=1)
        
        while df is None or df.empty and num_retries:
            num_retries -= 1
            self.logger.debug(f'downloading {data_model}')
            # NOTE: yfinance's period is not used, only use start_date and end_date for data clarity in storage
            df: pd.DataFrame | None = ticker.history(
                interval=eresolution,
                start=str(data_model.start_date),
                end=str(yfinance_end_date),
                **self._yfinance_kwargs
            )
            # REVIEW: for some unknown reason, yfinance sometimes returns None even start_date and end_date are within a valid range
            # so we need to increment start_date by 1 day to shorten the date range and try again
            if df is None or df.empty:
                closer_start_date = data_model.start_date + datetime.timedelta(days=1)
                if closer_start_date >= data_model.end_date:
                    break
                else:
                    data_model.update_start_date(closer_start_date)
                    self.logger.info(f'failed to download {data_model.product} {data_model.resolution} data, retrying with start_date={closer_start_date}')
                time.sleep(0.1)
            else:
                self.logger.debug(f'downloaded {data_model}')
                break
        else:
            self.logger.warning(f'failed to download {data_model.product} {data_model.resolution} data, '
                                f'please check if start_date={original_start_date} and end_date={data_model.end_date} is within the valid range')
        self._yfinance_kwargs.clear()
        # TODO: better handling of rate limit control?
        time.sleep(1)
        return df

    def get_historical_data(
        self,
        product: str,
        symbol: str='',
        resolution: Resolution | str = "1day",
        rollback_period: str | Literal["ytd", "max"] = "max",
        start_date: str = "",
        end_date: str = "",
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        data_origin: str='',
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
        yfinance_kwargs: dict | None=None,
        **product_specs,
    ) -> tDataFrame | None:
        """Gets historical data from Yahoo Finance using yfinance's Ticker.history().
        Args:
            product: product basis, e.g. AAPL_USD_STK, BTC_USDT_PERP
            symbol: symbol that will be used by yfinance's Ticker.history().
                If not specified, it will be derived from `product`, which might be inaccurate.
            rollback_period: Data resolution or 'ytd' or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1M' = 1 month.
                if 'period' in kwargs is specified, it will be used instead of `rollback_period`.
            resolution: Data resolution
                e.g. '1m' = 1 minute as the unit of each data bar/candle.
                Default is '1d' = 1 day.
            data_layer:
                'cleaned' (least raw): normalize data (refer to 'normalized' below), also remove all non-standard columns
                    e.g. standard columns in stock data are ts, product, open, high, low, close, volume, dividends, splits
                'normalized' (default): perform normalization following pfund's convention, preserve all columns
                    Normalization example:
                    - renaming: 'timestamp' -> 'date'
                    - mapping: 'buy' -> 1, 'sell' -> -1
                'original' (most raw): keep the original data from yfinance, no transformation will be performed.
                It will be ignored if the data is loaded from storage but not downloaded.
            yfinance_kwargs: kwargs supported by `yfinance`
                refer to kwargs in history() in yfinance/scrapers/history.py
        """
        self._yfinance_kwargs = self._check_yfinance_kwargs(yfinance_kwargs)
        df = super().get_historical_data(
            product,
            resolution,
            symbol=symbol,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            data_domain=data_domain,
            data_origin=data_origin,
            from_storage=from_storage,
            storage_configs=storage_configs,
            **product_specs,
        )
        self._yfinance_kwargs.clear()
        return df


    ###
    # Functions using yfinance for convenience
    ###
    def get_option_expirations(self, symbol: str) -> tuple[str]:
        '''Get all available option expirations for a given symbol.'''
        ticker: Ticker = self.api.Ticker(symbol)
        expirations = ticker.options
        return expirations

    # TODO: standardize the df? e.g. standardize column names, date format etc.
    def get_option_chain(self, symbol: str, expiration: str, option_type: Literal['CALL', 'PUT']) -> tDataFrame:
        '''Get the option chain for a given symbol, expiration, and option type.
        Args:
            expiration: e.g. '2024-12-13', it must be one of the values returned by `get_option_expirations`.
            option_type: 'CALL' or 'PUT'
        '''
        from pfeed._etl.base import convert_to_user_df
        
        ticker: Ticker = self.api.Ticker(symbol)
        option_chain = ticker.option_chain(expiration)
        if option_type.upper() == 'CALL':
            df = option_chain.calls
        elif option_type.upper() == 'PUT':
            df = option_chain.puts
        else:
            raise ValueError(f"Invalid option type: {option_type}")
        return convert_to_user_df(df, self.data_tool.name)