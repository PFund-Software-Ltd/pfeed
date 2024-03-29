import datetime

import pandas as pd
import yfinance as yf

from pfeed.feeds.base_feed import BaseFeed
from pfeed.config_handler import ConfigHandler


__all__ = ['YahooFinanceFeed']


class YahooFinanceFeed(BaseFeed):
    _ADAPTER = {
        'timeframe': {
            # pfund's : yfinance's
            'M': 'mo',
            'w': 'wk',
        }
    }
    # yfinance's valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
    SUPPORTED_TIMEFRAMES_AND_PERIODS = {
        'm': [1, 2, 5, 15, 30, 60, 90],
        'h': [1],
        'd': [1, 5],
        'w': [1],
        'M': [1, 3],
    }
    
    def __init__(self, config: ConfigHandler | None=None):
        super().__init__('yahoo_finance', config=config)
    
    def get_ticker(self, symbol):
        return yf.Ticker(symbol.upper())
    
    # TODO: should manipulate the input resolution and support e.g. '2d' resolution even it is not in the SUPPORTED_TIMEFRAMES_AND_PERIODS
    def get_historical_data(
        self, 
        symbol: str,
        rollback_period: str='1M', 
        resolution: str='1d', 
        start_date: str=None,
        end_date: str=None, 
        **kwargs
    ) -> pd.DataFrame:
        """Simple Wrapper of yfinance history().
        For the details of args and kwargs, please refer to https://github.com/ranaroussi/yfinance
        
        Args:
            **kwargs: kwargs supported by `yfinance`
        """
        from pfund.datas.resolution import Resolution
        
        # convert pfund's rollback_period format to yfinance's period
        rollback_period = Resolution(rollback_period)
        timeframe = repr(rollback_period.timeframe)
        etimeframe = self._ADAPTER['timeframe'].get(timeframe, timeframe)
        erollback_period = str(rollback_period.period) + etimeframe
        # if user is directly using yfinance variable `period`, use it
        if 'period' in kwargs:
            period = kwargs['period']
            del kwargs['period']
        else:
            period = erollback_period
            
        # convert pfund's resolution format to yfinance's interval
        resolution = Resolution(resolution)
        timeframe = repr(resolution.timeframe)
        etimeframe = self._ADAPTER['timeframe'].get(timeframe, timeframe)
        eresolution = str(resolution.period) + etimeframe
        # if user is directly using yfinance variable `interval`, use it
        if 'interval' in kwargs:
            interval = kwargs['interval']
            del kwargs['interval']
        else:
            interval = eresolution
        
        if start_date:
            # default for end_date is today
            end_date = end_date or datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d')
            assert start_date != end_date, f'{start_date=} and {end_date=} cannot be the same, end_date in yfinance is exclusive'    
        else:
            start_date = end_date = None
        
        ticker = self.get_ticker(symbol)
        df = ticker.history(period=period, interval=interval, start=start_date, end=end_date, **kwargs)
        
        assert not df.empty, f'No data found for {symbol=}, {resolution=}, {rollback_period=}, {start_date=}, {end_date=}. SUPPORTED_TIMEFRAMES_AND_PERIODS={self.SUPPORTED_TIMEFRAMES_AND_PERIODS}'
        
        df.rename_axis('ts', inplace=True)  # rename index 'Date' to 'ts'
        df.columns = df.columns.str.lower()
        # if there are spaces in column names, they will be turned into some weird names like "_10" 
        # during "for row in df.itertuples()"
        df = df.rename(columns={'stock splits': 'stock_splits'})
        # convert to UTC
        df.index = df.index.tz_convert('UTC')
        # convert to UTC and remove +hh:mm from YYYY-MM-DD hh:mm:ss+hh:mm
        df.index = df.index.tz_convert('UTC').tz_localize(None)
        
        df.insert(0, 'symbol', symbol)
        df.insert(1, 'resolution', repr(resolution))
        return df
