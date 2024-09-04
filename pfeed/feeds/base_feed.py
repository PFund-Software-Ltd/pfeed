from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    try:
        import pandas as pd
    except ImportError:
        pass
    from pfeed.types.common_literals import tSUPPORTED_DATA_TOOLS, tSUPPORTED_DATA_TYPES
    from pfund.datas.resolution import Resolution
    
import os
import io
import glob
import shutil
import logging
import datetime
import importlib
import tempfile

try:
    import polars as pl
except ImportError:
    pass

from pfeed.config_handler import get_config
from pfeed.const.common import SUPPORTED_DATA_FEEDS, SUPPORTED_DATA_TOOLS
from pfeed.utils.utils import get_dates_in_between, rollback_date_range
from pfeed.utils.validate import validate_pdt
from pfeed import etl


__all__ = ["BaseFeed"]


class BaseFeed:
    def __init__(self, name: str, data_tool: tSUPPORTED_DATA_TOOLS='pandas'):
        from pfund.plogging import set_up_loggers
        
        self.name = name.upper()
        assert self.name in SUPPORTED_DATA_FEEDS, f"Invalid {self.name=}, {SUPPORTED_DATA_FEEDS=}"
        self.data_tool = data_tool.lower()
        assert self.data_tool in SUPPORTED_DATA_TOOLS, f"Invalid {self.data_tool=}, {SUPPORTED_DATA_TOOLS=}"
        
        try:
            module = importlib.import_module(f'pfeed.sources.{self.name.lower()}')
            setattr(self, self.name.lower(), module)  # set e.g. 'bybit' module as attribute
            self.api = getattr(module, "api")
            self.const = getattr(module, "const")
            self._default_raw_dtype = self.const.SUPPORTED_RAW_DATA_TYPES[0]
        except (AttributeError, ModuleNotFoundError):
            self.api = self.const = self._default_raw_dtype = None

        config = get_config()
        is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
        if not is_loggers_set_up:
            set_up_loggers(config.log_path, config.logging_config_file_path, user_logging_config=config.logging_config)
        self.data_path = config.data_path
        self.logger = logging.getLogger(self.name.lower() + '_data')
        
        self.temp_dir = tempfile.mktemp(prefix=f"{self.name.lower()}_temp_", dir=self.data_path)

    def _derive_dtype_and_resolution(self, resolution: str):
        """Derive dtype and resolution from the given resolution string.
        For convenience, the resolution string could be a raw data type (e.g. 'raw', 'raw_tick') or resolution (e.g. '1d')
        where using 'raw' means the default raw data type will be used, that is the first element in SUPPORTED_RAW_DATA_TYPES,
        and the resolution string will be converted to a Resolution object.

        Args:
            resolution (str): Resolution string, e.g. '1d', '1h', '1m', '1s', 'raw', 'raw_tick', 'raw_second', 'raw_minute', 'raw_hour', 'raw_daily'.

        Returns:
            tuple[str, Resolution]: A tuple containing the dtype and resolution.
        """
        from pfund.datas.resolution import Resolution
        
        if resolution.startswith("raw"):
            dtype = resolution
            # e.g. convert 'raw' to 'raw_tick'
            if dtype == "raw":
                dtype = self._default_raw_dtype
            assert (
                dtype in self.const.SUPPORTED_RAW_DATA_TYPES
            ), f'"{dtype}" is not supported for {self.name} data, supported types are: {self.const.SUPPORTED_RAW_DATA_TYPES}'
            resolution = Resolution("1" + dtype.split("_")[-1])
        else:
            resolution = Resolution(resolution)
            if resolution.is_tick():
                assert resolution.period == 1, f"{resolution=} is not supported"
                dtype = "tick"
            elif resolution.is_second():
                dtype = "second"
            elif resolution.is_minute():
                dtype = "minute"
            elif resolution.is_hour():
                dtype = "hour"
            elif resolution.is_day():
                dtype = "daily"
            else:
                raise Exception(f"{resolution=} is not supported")
        return dtype, resolution

    def _get_data_from_local(
        self, pdt: str, date: str, dtype: tSUPPORTED_DATA_TYPES, resolution: Resolution,
    ) -> bytes | None:
        from pfund.datas.resolution import Resolution

        if local_data := etl.get_data(self.name, dtype, pdt, date, mode="historical"):
            local_data_dtype = dtype
            local_data_resolution = Resolution('1' + repr(resolution.timeframe))
        # if can't find local data with dtype e.g. second, check if raw data exists
        elif self._default_raw_dtype and not dtype.startswith('raw') and dtype != self._default_raw_dtype and \
            (local_data := etl.get_data(self.name, self._default_raw_dtype, pdt, date, mode="historical")):
            local_data_dtype = self._default_raw_dtype
            local_data_resolution = Resolution('1' + self._default_raw_dtype.split('_')[-1])
            assert local_data_resolution.timeframe <= resolution.timeframe, f"{resolution=} but the local raw data with the highest resolution is {local_data_resolution}, please use a lower resolution"
            self.logger.info(f'No local {self.name} data found with {dtype=}, switched to find "{local_data_dtype}" data instead')
        else:
            local_data = None
            return local_data
            
        data_str = f"{self.name} {pdt} {date} {local_data_dtype} data"
        self.logger.info(f"loaded {data_str} locally")

        if local_data_dtype == self._default_raw_dtype and local_data_dtype != dtype:
            return self._transform_raw_data_to_dtype(local_data, dtype, resolution)
        else:
            return local_data
    
    def _get_data_from_source(
        self, pdt: str, date: str, dtype: tSUPPORTED_DATA_TYPES, resolution: Resolution
    ) -> bytes | None:
        raise NotImplementedError(f"{self.name} _get_data_from_source() is not implemented")
    
    def _transform_raw_data_to_dtype(self, raw_data: bytes, dtype: tSUPPORTED_DATA_TYPES, resolution: Resolution) -> bytes:
        raise NotImplementedError(f"{self.name} _transform_raw_data_to_dtype() is not implemented")
    
    def _read_parquet(self, temp_file_path: str) -> pl.LazyFrame:
        """Read a parquet file into a lazyframe."""
        if self.data_tool == 'pandas':
            raise Exception(f"{self.data_tool=} is not supposed to use this method")
        elif self.data_tool == 'polars':
            lf = pl.scan_parquet(temp_file_path)
            return lf
    
    def _concat_dfs(self, dfs: list[pd.DataFrame | pl.LazyFrame]) -> pd.DataFrame | pl.LazyFrame:
        if self.data_tool == 'pandas':
            import pandas as pd
            return pd.concat(dfs)
        elif self.data_tool == 'polars':
            return pl.concat(dfs)
    
    def _prepare_temp_dir(self):
        """Remove old temp directories and create a new temp directory."""
        # remove old temp directories
        temp_dir_pattern = os.path.join(self.data_path, f"{self.name.lower()}_temp_*")
        for dir_path in glob.glob(temp_dir_pattern):
            try:
                if os.path.isdir(dir_path):
                    shutil.rmtree(dir_path)
                    self.logger.debug(f"Removed temporary directory: {dir_path}")
            except Exception as e:
                self.logger.error(f"Error removing directory {dir_path}: {str(e)}")
        if not os.path.exists(self.temp_dir):
            os.mkdir(self.temp_dir)
            
    def _organize_columns(self, pdt: str, resolution: Resolution, df: pd.DataFrame | pl.LazyFrame) -> pd.DataFrame | pl.LazyFrame:
        """Add product and resolution columns to the DataFrame and reorder the them."""
        if self.data_tool == 'pandas':
            df.insert(1, 'product', pdt)
            df.insert(2, 'resolution', repr(resolution))
        elif self.data_tool == 'polars':
            df = df.with_columns(
                pl.lit(pdt).alias('product'),
                pl.lit(repr(resolution)).alias('resolution')
            )
            # reorder columns
            left_cols = ['ts', 'product', 'resolution']
            df = df.select(left_cols + [col for col in df.collect_schema().names() if col not in left_cols])
        return df

    def _estimate_memory_usage(self, df: pd.DataFrame) -> float:
        if self.data_tool == 'pandas':
            return df.memory_usage(deep=True).sum() / (1024 ** 3)
        elif self.data_tool == 'polars':
            return pl.from_pandas(df).estimated_size(unit='gb')
    
    def get_historical_data(
        self,
        pdt: str,
        rollback_period: str = "1w",
        resolution: str
        | Literal[
            "raw", "raw_tick", "raw_second", "raw_minute", "raw_hour", "raw_daily"
        ] = "1d",
        start_date: str = "",
        end_date: str = "",
        show_memory_warning: bool = False,
    ) -> pd.DataFrame | pl.LazyFrame:
        """Get historical data from the data source.
        Args:
            pdt: Product symbol, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            rollback_period:
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1w' = 1 week.
            resolution: Data resolution or raw data type for convenience,
                e.g. '1m' = 1 minute as the unit of each data bar/candle.
                if resolution='raw'/'raw_tick', '1' will be added to the front of the raw data type to form a valid resolution string.
                Default is '1d' = 1 day.
            start_date: Start date.
            end_date: End date.
        """
        import pandas as pd
            
        assert validate_pdt(
            self.name, pdt
        ), f'"{pdt}" does not match the required format "XXX_YYY_PTYPE" or has an unsupported product type. (PTYPE means product type, e.g. PERP, Supported types for {self.name} are: {self.const.SUPPORTED_PRODUCT_TYPES})'
        dtype, resolution = self._derive_dtype_and_resolution(resolution)
        if start_date:
            yesterday = (
                datetime.datetime.now(tz=datetime.timezone.utc)
                - datetime.timedelta(days=1)
            ).strftime("%Y-%m-%d")
            end_date: str = end_date or yesterday
        else:
            start_date, end_date = rollback_date_range(rollback_period)
        
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        dfs = []  # could be dataframes or lazyframes
        total_estimated_memory_usage_in_gb = 0
        if self.data_tool != 'pandas':
            self._prepare_temp_dir()
        
        for date in dates:
            data = self._get_data_from_local(pdt, date, dtype, resolution) or self._get_data_from_source(pdt, date, dtype, resolution)
            if not data:
                raise Exception(f"No data found for {self.name} {pdt} {date} from local or source")

            df = pd.read_parquet(io.BytesIO(data))
            if show_memory_warning:
                total_estimated_memory_usage_in_gb += self._estimate_memory_usage(df)
                self.logger.warning(f"Estimated memory usage for {self.name} {pdt} {resolution=} from {start_date} to {date} is {total_estimated_memory_usage_in_gb:.2f} GB")

            if self.data_tool != 'pandas':
                # Create a temporary parquet file for lazyframe to point to
                temp_file_path = os.path.join(self.temp_dir, f"temp_{self.name}_{pdt}_{repr(resolution)}_{date.strftime('%Y%m%d')}.parquet")
                df.to_parquet(temp_file_path, compression='zstd')
                lf = self._read_parquet(temp_file_path)
                dfs.append(lf)
            else:
                dfs.append(df)
        df = self._concat_dfs(dfs)
        
        # Resample daily data
        # NOTE: Since the downloaded data is in daily units, we can't resample it to e.g. '2d' resolution
        # using the above logic. Need to resample the aggregated daily data to resolution '2d':
        if resolution.is_day() and resolution.period != 1:
            df = etl.resample_data(df, resolution)
            self.logger.info(f'resampled {self.name} {pdt} {date} {dtype} data to {resolution=}')
            
        if not dtype.startswith('raw'):
            df = self._organize_columns(pdt, resolution, df)
        
        return df
    
    def download_historical_data(
        self,
        pdts: str | list[str] | None = None,
        dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None = None,
        ptypes: str | list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        num_cpus: int = 8,
        use_ray: bool = True,
        use_minio: bool = False,
    ):
        try:
            data_source = getattr(self, self.name.lower())
            data_source.download_historical_data(
                pdts=pdts,
                dtypes=dtypes,
                ptypes=ptypes,
                start_date=start_date,
                end_date=end_date,
                num_cpus=num_cpus,
                use_ray=use_ray,
                use_minio=use_minio,          
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