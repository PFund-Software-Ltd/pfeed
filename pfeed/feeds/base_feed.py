from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS, tSUPPORTED_STORAGES, tSUPPORTED_DATA_TYPES
    from pfeed.types.core import tDataFrame
    from pfeed.resolution import ExtendedResolution
    
import os
import io
import glob
import shutil
import logging
import datetime
import importlib

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
            
    def _get_historical_data_from_storages(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, dates: list[datetime.date], storage: tSUPPORTED_STORAGES='') -> tDataFrame | None:
        from pfeed import etl
        default_raw_resolution = self.utils.get_default_raw_resolution()
        storages = [storage] if storage else SUPPORTED_STORAGES
        if (df := etl.get_data("BACKTEST", self.name, pdt, resolution, dates, storages=storages, trading_venue=trading_venue, output_format=self.data_tool.name)) is not None:
            return df
        # if can't find local data with resolution, check if raw data exists
        elif default_raw_resolution and not resolution.is_raw() and default_raw_resolution.is_ge(resolution) and \
            (df := etl.get_data("BACKTEST", self.name, pdt, default_raw_resolution, dates, storages=storages, trading_venue=trading_venue, output_format=self.data_tool.name)) is not None:
            self.logger.debug(f'No local {self.name} data found with {resolution=}, switched to find "{default_raw_resolution}" data instead')
            transformed_df = etl.transform_data(self.name, pdt, df, default_raw_resolution, resolution)
            self.logger.debug(f'transformed {self.name} raw data to {resolution=}')
            return transformed_df
    
    def _get_historical_data_from_temp(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, dates: list[datetime.date]) -> tDataFrame | None:
        from pfeed import etl
        default_raw_resolution = self.utils.get_default_raw_resolution()
        temp_file_paths = [self._create_temp_file_path(trading_venue, pdt, resolution, date) for date in dates]
        raw_temp_file_paths = [self._create_temp_file_path(trading_venue, pdt, default_raw_resolution, date) for date in dates]
        if all(os.path.exists(path) for path in temp_file_paths):
            self.logger.debug(f'loaded temporary parquet files from {dates[0]} to {dates[-1]}')
            return self.data_tool.read_parquet(temp_file_paths)
        elif default_raw_resolution and not resolution.is_raw() and default_raw_resolution.is_ge(resolution) and \
            all(os.path.exists(path) for path in raw_temp_file_paths):
            self.logger.debug(f'loaded temporary raw parquet files from {dates[0]} to {dates[-1]}')
            df = self.data_tool.read_parquet(raw_temp_file_paths)
            transformed_df = etl.transform_data(self.name, pdt, df, default_raw_resolution, resolution)
            self.logger.debug(f'transformed {self.name} raw data to {resolution=}')
            return transformed_df
    
    def _get_historical_data_from_source(
        self, trading_venue: str, pdt: str, resolution: ExtendedResolution, dates: list[datetime.date]
    ) -> list[bytes]:
        raise NotImplementedError(f"{self.name} _get_historical_data_from_source() is not implemented")
    
    def _get_historical_data(self, trading_venue: str, pdt: str, resolution: ExtendedResolution, dates: list[datetime.date], storage: tSUPPORTED_STORAGES='') -> tDataFrame:
        if (df := self._get_historical_data_from_storages(trading_venue, pdt, resolution, dates, storage=storage)) is not None:
            pass
        elif (df := self._get_historical_data_from_temp(trading_venue, pdt, resolution, dates)) is not None:
            pass
        elif datas := self._get_historical_data_from_source(trading_venue, pdt, resolution, dates):
            import pandas as pd
            temp_file_paths = []
            for data, date in zip(datas, dates):
                df = pd.read_parquet(io.BytesIO(data))
                temp_file_path = self._create_temp_file_path(trading_venue, pdt, resolution, date)
                temp_file_paths.append(temp_file_path)
                df.to_parquet(temp_file_path, compression='zstd')
                self.logger.debug(f'created temporary parquet file: {temp_file_path}')
            df = self.data_tool.read_parquet(temp_file_paths)
        return df
    
    def get_historical_data(
        self,
        product: str,
        resolution: str="1d",
        rollback_period: str="1w",
        start_date: str="",
        end_date: str="",
        trading_venue: str='',
        storage: tSUPPORTED_STORAGES='',
    ) -> tDataFrame:
        """Get historical data from the data source.
        Args:
            product: Product symbol, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            rollback_period:
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1w' = 1 week.
            resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
                Also supports raw resolution such as 'r1m', where 'r' stands for raw.            
                If resolution is 'raw', the default raw resolution of the data type will be used.
                Default is '1d' = 1 day.
            start_date: Start date.
            end_date: End date.
            trading_venue: trading venue's name, e.g. exchange's name or dapp's name
            storage: If specified, only search for data in the specified storage.
        """
        from pfeed import etl
        from pfeed.resolution import ExtendedResolution
        
        pdt, trading_venue, storage = product.upper(), trading_venue.upper(), storage.lower()
        assert validate_pdt(
            self.name, pdt
        ), f'"{pdt}" does not match the required format "XXX_YYY_PTYPE" or has an unsupported product type. (PTYPE means product type, e.g. PERP, Supported types for {self.name} are: {self.const.SUPPORTED_PRODUCT_TYPES})'
        if storage:
            assert storage in SUPPORTED_STORAGES, f"Invalid {storage=}, {SUPPORTED_STORAGES=}"
        self._prepare_temp_dir()
        if resolution == 'raw':
            assert self.const.SUPPORTED_DATA_TYPES[0].startswith('raw_')
            default_raw_dtype = self.const.SUPPORTED_DATA_TYPES[0]
            resolution = self.const.DTYPES_TO_RAW_RESOLUTIOS[default_raw_dtype]
        resolution = ExtendedResolution(resolution)
        trading_venue = trading_venue or derive_trading_venue(self.name)
        dates: list[datetime.date] = self._prepare_dates(start_date, end_date, rollback_period)
        df = self._get_historical_data(trading_venue, pdt, resolution, dates, storage=storage)

        is_resample_daily_data = resolution.is_day() and resolution.period != 1
        # NOTE: Since the downloaded data is in daily units, resample it to resolution e.g. '2d' if needed
        if is_resample_daily_data:
            df = etl.resample_data(df, resolution)
            self.logger.info(f'resampled {self.name} {pdt} data to {resolution=}')
            
        if not resolution.is_raw():
            df = self.data_tool.organize_time_series_columns(pdt, resolution, df, override_resolution=is_resample_daily_data)
        return df
    
    def download_historical_data(
        self,
        products: str | list[str] | None = None,
        dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None = None,
        ptypes: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        use_minio: bool = False,
        use_ray: bool = True,
        num_cpus: int = 8,
    ):
        try:
            data_source = getattr(self, self.name.lower())
            data_source.download_historical_data(
                products=products,
                dtypes=dtypes,
                ptypes=ptypes,
                start_date=start_date,
                end_date=end_date,
                use_minio=use_minio,          
                use_ray=use_ray,
                num_cpus=num_cpus,
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