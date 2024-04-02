"""High-level API for getting historical/streaming data from Bybit."""
import datetime

from typing import Literal

import polars as pl
import pandas as pd

from pfeed import etl
from pfeed.config_handler import ConfigHandler
from pfeed.const.commons import SUPPORTED_DATA_TOOLS
from pfeed.feeds.base_feed import BaseFeed
from pfeed.sources.bybit import api
from pfeed.sources.bybit.const import DATA_SOURCE, SUPPORTED_PRODUCT_TYPES, create_efilename, SUPPORTED_RAW_DATA_TYPES
from pfeed.utils.utils import get_dates_in_between, rollback_date_range
from pfeed.utils.validate import validate_pdt
from pfeed.data_tools.data_tool_polars import estimate_memory_usage
# from pfund.exchanges.bybit.exchange import Exchange


DataTool = Literal['pandas', 'polars', 'pyspark']


__all__ = ['BybitFeed']


class BybitFeed(BaseFeed):
    def __init__(self, config: ConfigHandler | None=None):
        super().__init__('bybit', config=config)
    
    def get_historical_data(
        self,
        pdt: str,
        rollback_period: str='1w',
        # HACK: mixing resolution with dtypes for convenience
        resolution: str | Literal['raw', 'raw_tick']='1d',  
        start_date: str=None,
        end_date: str=None,
        data_tool: Literal['pandas', 'polars', 'pyspark']='pandas',
        memory_usage_limit_in_gb: int=2,  # in GB
    ) -> pd.DataFrame | pl.LazyFrame:
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
            data_tool: Output DataFrame type
            memory_usage_limit_in_gb: Memory usage limit in GB for the output DataFrame. Default is 2 GB.
                If the memory usage exceeds the limit, the output DataFrame will be converted to a polars LazyFrame.
        """
        from pfund.datas.resolution import Resolution
        
        # exchange = Exchange(env='LIVE')
        # adapter = exchange.adapter
        # product = exchange.create_product(*pdt.split('_'))
        source = DATA_SOURCE
        assert validate_pdt(source, pdt), f'"{pdt}" does not match the required format "XXX_YYY_PTYPE" or has an unsupported product type. (PTYPE means product type, e.g. PERP, Supported types for {source} are: {SUPPORTED_PRODUCT_TYPES})'
        dtype = self._derive_dtype_from_resolution(resolution)
        if dtype.startswith('raw'):
            resolution = '1' + dtype.split('_')[-1]
        resolution = Resolution(resolution)
        data_tool = data_tool.lower()
        assert data_tool in SUPPORTED_DATA_TOOLS, f'Invalid {data_tool=}, {SUPPORTED_DATA_TOOLS=}'
        
        if start_date:
            # default for end_date is yesterday
            end_date: str = end_date or (datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date, end_date = rollback_date_range(rollback_period)
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        
        lfs = []  # list of lazy frames
        default_raw_dtype = SUPPORTED_RAW_DATA_TYPES[0]  # 'raw_tick' for bybit
        for date in dates:
            if local_data := etl.get_data(DATA_SOURCE, dtype, pdt, date, mode='historical'):
                local_data_dtype = dtype
            # if can't find local data with dtype e.g. second, check if raw data exists
            elif dtype != default_raw_dtype and (local_data := etl.get_data(DATA_SOURCE, default_raw_dtype, pdt, date, mode='historical')):
                local_data_dtype = default_raw_dtype
                self.logger.info(f'No local data found with {dtype=}, switch to find "{local_data_dtype}" data instead')
            else:
                local_data_dtype = ''
            
            if local_data:
                data_str = f'{source} {pdt} {date}'
                self.logger.info(f'loaded {data_str} {local_data_dtype} data locally')
                # REVIEW: last bar is very likely incomplete when e.g. 20 days of daily data is resampled to '3d'
                check_if_drop_last_bar = (local_data_dtype == dtype and resolution.period != 1)
                if local_data_dtype == dtype and resolution.period == 1:
                    data = local_data
                else:
                    if dtype != 'tick':
                        if dtype == 'daily' and resolution.period != 1:
                            # each data is in daily units, so resampling it to e.g. '2d' resolution directly is impossible, convert to '1d' first
                            data: bytes = etl.resample_data(local_data, '1d', check_if_drop_last_bar=False)
                        else:
                            data: bytes = etl.resample_data(local_data, resolution, check_if_drop_last_bar=check_if_drop_last_bar)
                    else:
                        data: bytes = etl.clean_raw_tick_data(local_data)
                    self.logger.warning(f'resampled local {data_str} data on the fly to {resolution=}, please consider storing "{dtype}" data locally')
            else:
                efilenames = api.get_efilenames(pdt)
                data_str = f'{source} {pdt} {date} {dtype}'
                if create_efilename(pdt, date) not in efilenames:
                    self.logger.info(f'{data_str} data is not found')
                    continue
                self.logger.warning(f"Downloading {data_str} data on the fly, please consider using pfeed's {source.lower()}.download(...) to pre-download data to your local computer first")
                if raw_data := api.get_data(pdt, date):
                    raw_tick: bytes = etl.clean_raw_data(DATA_SOURCE, raw_data)
                    if dtype == default_raw_dtype:
                        data = raw_tick
                    else:
                        tick_data: bytes = etl.clean_raw_tick_data(raw_tick)
                        if dtype != 'tick':
                            data: bytes = etl.resample_data(tick_data, resolution)
                            self.logger.info(f'resampled {data_str} data to {resolution=}')
                        else:
                            data = tick_data
                else:
                    raise Exception(f'failed to download {data_str} historical data, please check your network connection')
            
            df = pl.read_parquet(data)
            lf = df.lazy()
            lfs.append(lf)
        
        lf = pl.concat(lfs)
        
        # NOTE: Since the downloaded data is in daily units, we can't resample it to e.g. '2d' resolution
        # using the above logic. Need to resample the aggregated daily data to resolution '2d':
        if dtype == 'daily' and resolution.period != 1:
            data: bytes = lf.collect().to_pandas().to_parquet()
            data: bytes = etl.resample_data(data, resolution, check_if_drop_last_bar=True)
            lf = pl.read_parquet(data).lazy()
                
        if not dtype.startswith('raw'):
            lf = lf.with_columns(
                pl.lit(pdt).alias('product'),
                pl.lit(repr(resolution)).alias('resolution')
            )
            # reorder columns
            left_cols = ['ts', 'product', 'resolution']
            lf = lf.select(left_cols + [col for col in df.columns if col not in left_cols])
        
        estimated_memory_usage_in_gb = estimate_memory_usage(lf)
        if estimated_memory_usage_in_gb < memory_usage_limit_in_gb:
            df = lf.collect()
            if data_tool in ['pandas', 'polars']:
                return df.to_pandas() if data_tool == 'pandas' else df
            # TODO
            elif data_tool == 'pyspark':
                raise NotImplementedError('pyspark is not supported yet')
        else:
            self.logger.warning(f'Output DataFrame estimated memory usage {estimated_memory_usage_in_gb:.2f} GB exceeds the limit of {memory_usage_limit_in_gb} GB, returning a LazyFrame')
            if data_tool in ['pandas', 'polars']:
                return lf
            # TODO
            elif data_tool == 'pyspark':
                raise NotImplementedError('pyspark is not supported yet')
    
    # TODO?: maybe useful if used as a standalone program, not useful at all if used with PFund
    def get_realtime_data(self, env='LIVE'):
        pass
