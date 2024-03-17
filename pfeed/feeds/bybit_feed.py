"""High-level API for getting historical/streaming data from Bybit."""
import io
import datetime

from typing import Literal

import pandas as pd

from pfeed.config_handler import ConfigHandler
from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.bybit import api
from pfeed.sources.bybit import etl
from pfeed.sources.bybit.const import DATA_SOURCE, SUPPORTED_PRODUCT_TYPES, create_efilename
from pfeed.utils.utils import get_dates_in_between, rollback_date_range
from pfeed.utils.validate import validate_pdt
# from pfund.exchanges.bybit.exchange import Exchange


__all__ = ['BybitFeed']


class BybitFeed(BaseFeed):
    def __init__(self, config: ConfigHandler | None=None):
        super().__init__('bybit', config=config)
    
    def get_historical_data(
        self,
        pdt: str,
        rollback_period: str='1w',
        # HACK: mixing resolution with dtype for convenience
        resolution: str | Literal['raw', 'raw_tick']='1d',  
        start_date: str=None,
        end_date: str=None,
        only_ohlcv: bool=False,
    ) -> pd.DataFrame:
        """Get historical data from Bybit.
        Args:
            pdt: Product symbol, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            rollback_period: 
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1w' = 1 week.
            resolution: Data resolution, 
                e.g. '1m' = 1 minute as the unit of each data bar/candle.
                if resolution='raw'/'raw_tick', 
                return the downloaded raw tick data from Bybit directly after standardizing the format.
                Default is '1d' = 1 day.
            start_date: Start date.
            end_date: End date.
            only_ohlcv: If True, only return OHLCV columns.
        """
        from pfund.datas.resolution import Resolution
        
        # exchange = Exchange(env='LIVE')
        # adapter = exchange.adapter
        # product = exchange.create_product(*pdt.split('_'))
        source = DATA_SOURCE
        assert validate_pdt(source, pdt), f'"{pdt}" does not match the required format "XXX_YYY_PTYPE" or has an unsupported product type. (PTYPE means product type, e.g. PERP, Supported types for {source} are: {SUPPORTED_PRODUCT_TYPES})'
        dtype = self._derive_dtype_from_resolution(resolution)
        efilenames = api.get_efilenames(pdt)
        if start_date:
            # default for end_date is yesterday
            end_date: str = end_date or (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date, end_date = rollback_date_range(rollback_period)
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        
        dfs = []
        dates = [date for date in dates if create_efilename(pdt, date) in efilenames]
        for date in dates:
            if local_data := etl.get_data(dtype, pdt, date, mode='historical'):
                local_data_dtype = dtype
            # if can't find local data with dtype e.g. second, check if raw data exists
            elif dtype != 'raw_tick' and (local_data := etl.get_data('raw_tick', pdt, date, mode='historical')):
                local_data_dtype = 'raw_tick'
                self.logger.warning(f'No local data found with {dtype=}, switch to find "{local_data_dtype}" data instead')
            else:
                local_data_dtype = ''
            
            if local_data:
                data_str = f'{source} {pdt} {date}'
                self.logger.info(f'loaded {data_str} {local_data_dtype} data locally')
                if local_data_dtype == dtype:
                    data = local_data
                else:
                    if dtype != 'tick':
                        data: bytes = etl.resample_data(local_data, resolution, only_ohlcv=only_ohlcv)
                    else:
                        data: bytes = etl.clean_raw_tick_data(local_data)
                    self.logger.info(f'resampled {data_str} data to {resolution=}')
            else:
                data_str = f'{source} {pdt} {date} {dtype}'
                self.logger.warning(f"Downloading {data_str} data on the fly, please consider using pfeed's {source.lower()}.download(...) to pre-download data to your local computer first")
                if raw_data := api.get_data(pdt, date):
                    raw_tick: bytes = etl.clean_raw_data(raw_data)
                    if dtype == 'raw_tick':
                        data = raw_tick
                    else:
                        tick_data: bytes = etl.clean_raw_tick_data(raw_tick)
                        if dtype != 'tick':
                            data: bytes = etl.resample_data(tick_data, resolution, only_ohlcv=only_ohlcv)
                            self.logger.info(f'resampled {data_str} data to {resolution=}')
                        else:
                            data = tick_data
                else:
                    raise Exception(f'failed to download {data_str} historical data, please check your network connection')
            df = pd.read_parquet(io.BytesIO(data))
            dfs.append(df)
        
        df = pd.concat(dfs)
        
        # NOTE: Since the downloaded data is in daily units, we can't resample it to e.g. '2d' resolution
        # using the above logic. Need to resample the aggregated daily data to resolution '2d':
        if dtype == 'daily' and resolution != '1d':
            data: bytes = df.to_parquet(compression='snappy')
            resampled_data: bytes = etl.resample_data(data, resolution, only_ohlcv=only_ohlcv)
            df = pd.read_parquet(io.BytesIO(resampled_data))
        
        df.insert(0, 'product', pdt)
        df.insert(1, 'resolution', repr(Resolution(resolution)))
        
        return df
    
    # TODO?: maybe useful if used as a standalone program, not useful at all if used with PFund
    def get_real_time_data(self, env='LIVE'):
        pass
