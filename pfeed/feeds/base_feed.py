from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    try:
        import pandas as pd
        import polars as pl
    except ImportError:
        pass
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS, tSUPPORTED_STORAGES
    from pfeed.sources.bybit.types import tSUPPORTED_DATA_TYPES
    from pfeed.resolution import ExtendedResolution
    DataFrame = pd.DataFrame | pl.LazyFrame
    
import os
import io
import glob
import shutil
import logging
import datetime
import importlib
from logging.handlers import QueueHandler, QueueListener

try:
    import polars as pl
except ImportError:
    pass

from pfeed.config_handler import get_config
from pfeed.const.common import SUPPORTED_DATA_FEEDS, SUPPORTED_DATA_TOOLS, SUPPORTED_STORAGES
from pfeed.utils.utils import (
    get_dates_in_between, 
    rollback_date_range, 
    derive_trading_venue,
)
from pfeed.utils.validate import validate_pdt


__all__ = ["BaseFeed"]


class BaseFeed:
    def __init__(self, name: str, data_tool: tSUPPORTED_DATA_TOOLS='pandas'):
        '''
        Args:
            from_storage: If specified, only search for data in the specified storage.
        '''
        from pfund.plogging import set_up_loggers

        self.name = name.upper()
        data_tool = data_tool.lower()
        assert self.name in SUPPORTED_DATA_FEEDS, f"Invalid {self.name=}, {SUPPORTED_DATA_FEEDS=}"
        assert data_tool in SUPPORTED_DATA_TOOLS, f"Invalid {data_tool=}, {SUPPORTED_DATA_TOOLS=}"
        self.data_tool = importlib.import_module(f'pfeed.data_tools.data_tool_{data_tool}')
        try:
            module = importlib.import_module(f'pfeed.sources.{self.name.lower()}')
            setattr(self, self.name.lower(), module)  # set e.g. 'bybit' module as attribute
            self.api = getattr(module, "api")
            self.const = getattr(module, "const")
            self.utils = getattr(module, "utils")
            assert self.const.SUPPORTED_DATA_TYPES[0].startswith('raw')
        except (AttributeError, ModuleNotFoundError):
            self.api = self.const = None
        
        config = get_config()
        is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
        if not is_loggers_set_up:
            set_up_loggers(config.log_path, config.logging_config_file_path, user_logging_config=config.logging_config)
        self.data_path = config.data_path
        self.logger = logging.getLogger(self.name.lower() + '_data')
        self.temp_dir = self._create_temp_dir()
    
    def _create_temp_dir(self):
        current_date = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%d')
        temp_dir_name = f"{self.name.lower()}_temp_{current_date}"
        return os.path.join(self.data_path, temp_dir_name)
    
    def _create_temp_file_path(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, date: datetime.date) -> str:
        return os.path.join(self.temp_dir, f"temp_{self.name}_{trading_venue.upper()}_{pdt.upper()}_{repr(resolution)}_{date.strftime('%Y%m%d')}.parquet")
    
    def _prepare_temp_dir(self):
        """Remove old temp directories and create a new temp directory."""
        today = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%d')
        temp_dir_pattern = os.path.join(self.data_path, f"{self.name.lower()}_temp_*")
        for dir_path in glob.glob(temp_dir_pattern):
            try:
                if os.path.isdir(dir_path) and today not in dir_path:
                    shutil.rmtree(dir_path)
                    self.logger.debug(f"Removed temporary directory: {dir_path}")
            except Exception as e:
                self.logger.error(f"Error removing directory {dir_path}: {str(e)}")
        if self.temp_dir and not os.path.exists(self.temp_dir):
            os.mkdir(self.temp_dir)
            
    @staticmethod
    def _prepare_dates(start_date: str | None, end_date: str | None, rollback_period: str) -> list[datetime.date]:
        if start_date:
            yesterday = (
                datetime.datetime.now(tz=datetime.timezone.utc)
                - datetime.timedelta(days=1)
            ).strftime("%Y-%m-%d")
            end_date: str = end_date or yesterday
        else:
            start_date, end_date = rollback_date_range(rollback_period)
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        return dates
            
    def _get_historical_data_from_storages(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, date: datetime.date, from_storage: tSUPPORTED_STORAGES='') -> DataFrame | None:
        from pfeed import etl
        default_raw_resolution = self.utils.get_default_raw_resolution()
        storages = [from_storage] if from_storage else SUPPORTED_STORAGES
        if (data := etl.get_data("BACKTEST", self.name, pdt, resolution, date, storages=storages, trading_venue=trading_venue, output_format=self.data_tool.name)) is not None:
            return data
        # if can't find local data with resolution, check if raw data exists
        elif default_raw_resolution and not resolution.is_raw() and default_raw_resolution.is_ge(resolution) and \
            (data := etl.get_data("BACKTEST", self.name, pdt, default_raw_resolution, date, storages=storages, trading_venue=trading_venue, output_format=self.data_tool.name)) is not None:
            self.logger.debug(f'No local {self.name} data found with {resolution=}, switched to find "{default_raw_resolution}" data instead')
            transformed_data = etl.transform_data(self.name, data, default_raw_resolution, resolution)
            self.logger.debug(f'transformed {self.name} raw data to {resolution=}')
            return transformed_data
    
    def _get_historical_data_from_temp(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, date: datetime.date) -> DataFrame | None:
        temp_file_path = self._create_temp_file_path(trading_venue, pdt, resolution, date)
        if os.path.exists(temp_file_path):
            self.logger.debug(f'loaded temporary parquet file: {temp_file_path}')
            return self.data_tool.read_parquet(temp_file_path)
    
    def _get_historical_data_from_source(
        self, trading_venue: str, pdt: str, resolution: ExtendedResolution, date: datetime.date
    ) -> bytes | None:
        raise NotImplementedError(f"{self.name} _get_historical_data_from_source() is not implemented")
    
    def _get_historical_data(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, date: datetime.date, from_storage: tSUPPORTED_STORAGES='') -> DataFrame:
        import pandas as pd
        if (df := self._get_historical_data_from_storages(trading_venue, pdt, resolution, date, from_storage=from_storage)) is not None:
            pass
        elif (df := self._get_historical_data_from_temp(trading_venue, pdt, resolution, date)) is not None:
            pass
        elif data := self._get_historical_data_from_source(trading_venue, pdt, resolution, date):
            df = pd.read_parquet(io.BytesIO(data))
            temp_file_path = self._create_temp_file_path(trading_venue, pdt, resolution, date)
            df.to_parquet(temp_file_path, compression='zstd')
            self.logger.debug(f'created temporary parquet file: {temp_file_path}')
            if self.data_tool.name != 'pandas':
                # read_parquet will return a lazyFrame for e.g. polars
                df = self.data_tool.read_parquet(temp_file_path)
        else:
            raise Exception(f"No data found for {self.name} {pdt} {date} from local, temp, or source")
        return df
    
    def get_historical_data(
        self,
        pdt: str,
        rollback_period: str="1w",
        resolution: str="1d",
        start_date: str="",
        end_date: str="",
        trading_venue: str='',
        from_storage: tSUPPORTED_STORAGES='',
        show_memory_warning: bool=False,
    ) -> DataFrame:
        """Get historical data from the data source.
        Args:
            pdt: Product symbol, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            rollback_period:
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1w' = 1 week.
            resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
                Also supports raw resolution such as 'r1m', where 'r' stands for raw.            
                Default is '1d' = 1 day.
            start_date: Start date.
            end_date: End date.
            trading_venue: trading venue's name, e.g. exchange's name or dapp's name
            from_storage: If specified, only search for data in the specified storage.
            show_memory_warning: Whether to show memory usage warning.
        """
        from pfeed import etl
        from pfeed.resolution import ExtendedResolution
        
        pdt, trading_venue, from_storage = pdt.upper(), trading_venue.upper(), from_storage.lower()
        assert validate_pdt(
            self.name, pdt
        ), f'"{pdt}" does not match the required format "XXX_YYY_PTYPE" or has an unsupported product type. (PTYPE means product type, e.g. PERP, Supported types for {self.name} are: {self.const.SUPPORTED_PRODUCT_TYPES})'
        assert from_storage in SUPPORTED_STORAGES, f"Invalid {from_storage=}, {SUPPORTED_STORAGES=}"
        resolution = ExtendedResolution(resolution)
        trading_venue = trading_venue or derive_trading_venue(self.name)
        self._prepare_temp_dir()
        dates: list[datetime.date] = self._prepare_dates(start_date, end_date, rollback_period)
        
        dfs = []  # could be dataframes or lazyframes
        total_estimated_memory_usage_in_gb = 0

        for date in dates:
            df = self._get_historical_data(trading_venue, pdt, resolution, date, from_storage=from_storage)
            dfs.append(df)
            if show_memory_warning:
                total_estimated_memory_usage_in_gb += self.data_tool.estimate_memory_usage(df)
                self.logger.warning(f"Estimated memory usage for {self.name} {pdt} {resolution=} from {start_date} to {date} is {total_estimated_memory_usage_in_gb:.2f} GB")

        df = self.data_tool.concat(dfs)
        
        # Resample daily data
        # NOTE: Since the downloaded data is in daily units, we can't resample it to e.g. '2d' resolution
        # using the above logic. Need to resample the aggregated daily data to resolution '2d':
        if resolution.is_day() and resolution.period != 1:
            df = etl.resample_data(df, resolution)
            self.logger.info(f'resampled {self.name} {pdt} {date} daily data to {resolution=}')
            
        # move 'ts', 'product' and 'resolution' columns to the leftmost
        df = self.data_tool.organize_time_series_columns(pdt, resolution, df)
        return df
    
    def download_historical_data(
        self,
        pdts: str | list[str] | None = None,
        dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None = None,
        ptypes: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        use_minio: bool = False,
        use_ray: bool = True,
        ray_num_cpus: int = 8,
        ray_batch_size: int | None = None,
    ):
        try:
            data_source = getattr(self, self.name.lower())
            data_source.download_historical_data(
                pdts=pdts,
                dtypes=dtypes,
                ptypes=ptypes,
                start_date=start_date,
                end_date=end_date,
                use_minio=use_minio,          
                use_ray=use_ray,
                ray_num_cpus=ray_num_cpus,
                ray_batch_size=ray_batch_size,
            )
        except AttributeError:
            raise Exception(f'{self.name} does not support download_historical_data()')
    
    # TODO
    def stream_realtime_data(self):
        try:
            data_source = getattr(self, self.name.lower())
            data_source.stream_realtime_data()
        except AttributeError:
            raise Exception(f'{self.name} does not support stream_realtime_data()')
    
    download = download_historical_data
    stream = stream_realtime_data