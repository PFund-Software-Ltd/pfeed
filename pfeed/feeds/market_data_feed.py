from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from pfund.products.product_base import BaseProduct
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tSTORAGE, tPRODUCT_TYPE
    from pfeed.flows.dataflow import DataFlow
    from pfeed.storages.base_storage import BaseStorage

import os
import datetime
from threading import Thread
from queue import Queue
import logging
from logging.handlers import QueueHandler, QueueListener

import pandas as pd
import narwhals as nw
from rich.console import Console

from pfund import print_warning
from pfund.datas.resolution import Resolution
from pfeed import etl
from pfeed.feeds.base_feed import BaseFeed, clear_current_dataflows
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.const.enums import DataRawLevel, MarketDataType, DataAccessType
from pfeed.utils.utils import lambda_with_name


class MarketDataFeed(BaseFeed):
    def _print_download_msg(self, resolution: Resolution, start_date: datetime.date, end_date: datetime.date, raw_level: DataRawLevel):
        Console().print(f'Downloading historical {resolution} data from {self.name}, from {str(start_date)} to {str(end_date)} (UTC), raw_level={raw_level.name}', style='bold yellow')
    
    def _print_original_raw_level_msg(self):
        Console().print(
            f'Warning: {self.name} data with raw_level="original" will NOT be compatible with pfund backtesting. \n'
            'Use it only for data exploration or if you plan to use other backtesting frameworks.',
            style='bold magenta'
        )
        
    def _is_resample_required(self, resolution: Resolution) -> bool:
        if resolution > self.source.highest_resolution:
            raise ValueError(f'{resolution=} is not supported for {self.name}')
        elif resolution < self.source.lowest_resolution:
            return True
        elif resolution.period != 1:
            return True
        return False
    
    def create_market_data_model(
        self,
        product: BaseProduct,
        resolution: str | Resolution,
        raw_level: DataRawLevel,
        start_date: datetime.date,
        end_date: datetime.date | None = None,
        unique_identifier: str = '',
        compression: str = 'zstd',
        filename_prefix: str = '',
        filename_suffix: str = '',
    ) -> MarketDataModel:
        from pfeed.const.enums import Environment
        return MarketDataModel(
            env=Environment.BACKTEST,
            source=self.source,
            unique_identifier=unique_identifier,
            product=product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date,
            compression=compression,
            metadata=self._create_metadata(raw_level),
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
        )
    
    @clear_current_dataflows
    def download(
        self,
        products: str | list[str] | None=None,
        symbols: str | list[str] | None=None,
        product_types: tPRODUCT_TYPE | list[tPRODUCT_TYPE] | None=None, 
        data_type: Literal['quote_l3', 'quote_l2', 'quote_l1', 'quote', 'tick', 'second', 'minute', 'hour', 'day']='tick',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        to_storage: tSTORAGE='local',
        filename_prefix: str='',
        filename_suffix: str='',
        product_specs: dict[str, dict] | None=None,  # {'product_basis': {'attr': 'value', ...}}
    ) -> MarketDataFeed:
        '''
        Download historical data from data source.
        
        Args:
            filename_prefix: The prefix of the filename.
            filename_suffix: The suffix of the filename.
            product_specs: The specifications for the products.
                'TSLA_USD_OPT' is in `products`, you need to provide the specifications of the option in `product_specs`:
                e.g. {'TSLA_USD_OPT': {'strike_price': 500, 'expiration': '2024-01-01', 'option_type': 'CALL'}}
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
            rollback_period: Data resolution or 'ytd' (year to date) or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use yesterday's date as the default start date.
            end_date: End date.
                If not specified, use today's date as the end date.
        '''
        data_type = MarketDataType[data_type.upper()]
        resolution = Resolution(data_type.value)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        products = self._prepare_products(products, ptypes=product_types)
        if symbols:
            assert len(symbols) == len(products), "The number of symbols must match the number of products"
        else:
            symbols = [''] * len(products)
        specs = product_specs or {}
        raw_level = DataRawLevel[raw_level.upper()]
        is_resample_required = self._is_resample_required(resolution)
        if raw_level != DataRawLevel.CLEANED and is_resample_required:
            raw_level = DataRawLevel.CLEANED
            self.logger.info(
                f'raw_level is set to {raw_level.name} when resampling is required, '
                f'i.e. {resolution=} is not natively supported by {self.name}'
            )
        if self.config.print_msg and start_date and end_date:
            self._print_download_msg(resolution, start_date, end_date, raw_level)
        dataflows_per_pdt: dict[str, list[DataFlow]] = {}
        for product_basis, symbol in zip(products, symbols):
            product = self.create_product(product_basis, symbol=symbol, **specs.get(product_basis, {}))
            dataflows_per_pdt[product.name] = []
            dataflows: list[DataFlow] = self._create_download_dataflows(
                product,
                resolution,
                raw_level,
                start_date,
                end_date,
                filename_prefix,
                filename_suffix,
            )
            dataflows_per_pdt[product.name].extend(dataflows)
        self._add_default_transformations_to_download(dataflows_per_pdt, resolution, raw_level, is_resample_required)
        if not self._pipeline_mode:
            self.load(to_storage)
            self.run()
        return self
    
    def _create_download_dataflows(
        self,
        product: BaseProduct,
        resolution: Resolution,
        raw_level: DataRawLevel,
        start_date: datetime.date,
        end_date: datetime.date | None,
        filename_prefix: str,
        filename_suffix: str,
    ) -> list[DataFlow]:
        raise NotImplementedError
    
    def _create_metadata(self, raw_level: DataRawLevel) -> dict:
        # if is_placeholder is true, it means there is no data on that date
        # without it, you can't know if the data is missing due to download failure or there is actually no data on that date
        return {'raw_level': raw_level.name.lower(), 'is_placeholder': 'false'}
    
    # EXTEND: this is some basic data quality checks, use sth like "pandera" to do more comprehensive checks
    # e.g. "high" > "low", some columns must be positive and numeric, detect anomalous price movements to catch potentially erroneous data
    def _assert_data_quality(self, df: pd.DataFrame, data_model: MarketDataModel) -> pd.DataFrame:
        '''Asserts that the data conforms to pfeed's internal standards before loading it into storage.'''
        metadata, resolution = data_model.metadata, data_model.resolution
        raw_level = DataRawLevel[metadata['raw_level'].upper()]
        if raw_level == DataRawLevel.ORIGINAL:
            return df
        assert isinstance(df.loc[0, 'ts'], datetime.datetime), 'ts must be of datetime type'
        assert df['ts'].is_monotonic_increasing, 'ts must be sorted in ascending order'
        required_columns = {'ts', 'product', 'resolution'}
        assert required_columns.issubset(df.columns), f'Missing required columns {required_columns}'
        if resolution.is_quote():  # TODO: add support for quote data
            raise NotImplementedError('quote data is not supported')
        elif resolution.is_tick():
            assert {'price', 'side', 'volume'}.issubset(df.columns), "Missing 'price', 'side', 'volume' columns"
        else:
            assert {'open', 'high', 'low', 'close', 'volume'}.issubset(df.columns), "Missing 'open', 'high', 'low', 'close', 'volume' columns"
        return df
    
    def load(self, storage: tSTORAGE='local', dataflows: list[DataFlow] | None=None, **kwargs):
        # convert back to pandas dataframe before calling etl.write_data() in load()
        self.transform(etl.convert_to_pandas_df)
        super().load(storage, dataflows=dataflows, **kwargs)
    
    def _add_default_transformations_to_download(
        self,
        dataflows_per_pdt: dict[str, list[DataFlow]],
        resolution: Resolution,
        raw_level: DataRawLevel,
        is_resample_required: bool,
    ):
        self.transform(etl.convert_to_pandas_df)
        if raw_level != DataRawLevel.ORIGINAL:
            self.transform(self._normalize_raw_data)
            for product_name in dataflows_per_pdt:
                self.transform(
                    lambda_with_name('standardize_columns', lambda df: etl.standardize_columns(df, resolution, product_name)),
                    dataflows=dataflows_per_pdt[product_name],
                )
            if raw_level == DataRawLevel.CLEANED:
                transformations = [etl.filter_columns]
                # only resample if raw_level is 'cleaned', otherwise, can't resample non-standard columns
                if is_resample_required:
                    transformations.append(
                        lambda_with_name('resample_data', lambda df: etl.resample_data(df, resolution))
                    )
                self.transform(*transformations)
            self.transform(etl.organize_columns)
        else:
            self._print_original_raw_level_msg()
        if self._pipeline_mode:
            self.transform(
                lambda_with_name('convert_to_user_df', lambda df: etl.convert_to_user_df(df, self.data_tool.name))
            )
    
    def _get_historical_data_from_storage(
        self,
        product: BaseProduct,
        unit_resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        raw_level: DataRawLevel,
        unique_identifier: str = '',
        from_storage: tSTORAGE | None=None,
    ) -> tuple[Frame | None, list[datetime.date]]:
        from pfeed.utils.utils import get_dates_in_between
        # e.g. search data in order e.g. '3m' converted to '1m' -> search '1m' -> search '1t'
        assert unit_resolution.period == 1, 'unit_resolution must have period = 1'
        all_dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        search_resolutions: set[Resolution] = set([unit_resolution] + unit_resolution.get_higher_resolutions(exclude_quote=True))
        search_storages = ['cache', 'local', 'minio'] if from_storage is None else [from_storage]  

        def _search(date: datetime.date, queue: Queue) -> list[tuple[Resolution, BaseStorage]]:
            '''Search for data across all resolutions and storages for a given date.
            Stops searching once first valid result is found.
            '''
            for resolution in search_resolutions:
                for _from_storage in search_storages:
                    data_model = self.create_market_data_model(product, resolution, raw_level, date, unique_identifier=unique_identifier)
                    if storage := etl.extract_data(data_model, storage=_from_storage):
                        self.logger.debug(f'loaded {data_model} from {storage.name}')
                        queue.put(storage)
                        return
        
        num_dates = len(all_dates)
        MAX_THREADS = 64
        io_multiplier = 4
        num_threads = min(os.cpu_count() * io_multiplier, num_dates, MAX_THREADS)
        queue = Queue()
        data_storages: list[BaseStorage] = []
        for i in range(0, num_dates, num_threads):
            batch_dates = all_dates[i:i + num_threads]
            threads = []
            for date in batch_dates:
                # NOTE: set d=date to avoid lambda late binding issue
                thread = Thread(target=lambda d=date: _search(d, queue))
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()
            while not queue.empty():
                data_storages.append(queue.get())
        
        data_dates = [storage.date for storage in data_storages]
        missing_dates = sorted(list(set(all_dates) - set(data_dates)))
        if missing_dates:
            # since data on missing dates will be downloaded from source using start_date and end_date,
            # downloaded data will be consecutive, so need to make missing dates consecutive as well
            missing_dates = get_dates_in_between(missing_dates[0], missing_dates[-1])
        data_storages = [storage for storage in data_storages if storage.date not in missing_dates]
        data_storages.sort(key=lambda storage: storage.date)  # sort now so that final df doesn't need to be sorted
        data_dates = [storage.date for storage in data_storages]
        assert len(missing_dates) + len(data_dates) == num_dates, "Unexpected inconsistency in dates, please report this issue on github"
        
        def _get_df(storage: BaseStorage) -> tDataFrame:
            data_resolution = storage.data_model.resolution
            is_resample_required = unit_resolution < data_resolution
            df: tDataFrame = self.data_tool.read_parquet(storage.file_path, storage=storage.name.value)
            if is_resample_required:
                self.logger.debug(f'resampling {product.name} {storage.date} {data_resolution} data to {unit_resolution}')
                df: tDataFrame = etl.resample_data(df, unit_resolution)
            return df
        
        if not self._use_ray:
            dfs = [_get_df(storage) for storage in data_storages]
        else:
            import atexit
            import ray
            from ray.util.queue import Queue as RayQueue
            atexit.register(lambda: ray.shutdown())  # useful in jupyter notebook environment

            @ray.remote
            def ray_task(storage: BaseStorage) -> tDataFrame:
                if not self.logger.handlers:
                    self.logger.addHandler(QueueHandler(log_queue))
                    self.logger.setLevel(logging.DEBUG)
                return _get_df(storage)
            
            try:
                self._init_ray()
                log_queue = RayQueue()
                log_listener = QueueListener(log_queue, *self.logger.handlers, respect_handler_level=True)
                log_listener.start()
                futures = [ray_task.remote(storage) for storage in data_storages]
                dfs = ray.get(futures)
            except Exception:
                self.logger.exception(f'Error in getting {product.name} data from storage:')
            finally:
                if log_listener:
                    log_listener.stop()
                self._shutdown_ray()

        dfs: list[Frame] = [nw.from_native(df) for df in dfs]
        df: Frame | None = nw.concat(dfs) if dfs else None
        return df, missing_dates
    
    def _get_historical_data_from_source(
        self, 
        product: BaseProduct,
        unit_resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        raw_level: DataRawLevel,
        unique_identifier: str = '',
    ) -> Frame:
        if self.source.access_type == DataAccessType.PAID_BY_USAGE:
            raise Exception(
                f'{self.name} data is paid by usage, '
                f'you should use download() to download the data first '
                f'before calling get_historical_data(), '
                f'current missing dates in storage: from {start_date} to {end_date}'
            )
        assert unit_resolution.period == 1, 'unit_resolution must have period = 1'
        self.download(
            data_type=str(unit_resolution.timeframe).lower(),
            products=product.basis,
            start_date=str(start_date),
            end_date=str(end_date),
            raw_level=raw_level.name,
            to_storage='cache',
            product_specs={
                product.basis: product.specs
            },
        )
        df, missing_dates = self._get_historical_data_from_storage(
            product,
            unit_resolution,
            start_date,
            end_date,
            raw_level,
            unique_identifier=unique_identifier,
            from_storage='cache',
        )
        # after downloading, there should be no missing data
        assert not missing_dates, f'{product.name} {unit_resolution} data is missing for dates: {missing_dates}'
        return df

    def get_historical_data(
        self,
        product: str,
        symbol: str='',
        resolution: str="1d",
        rollback_period: str="1w",
        start_date: str="",
        end_date: str="",
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        from_storage: tSTORAGE | None=None,
        unique_identifier: str='',
        **product_specs
    ) -> tDataFrame | None:
        """Get historical data from the data source, local storage or cache.
        Args:
            product: Financial product, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            symbol: Symbol, e.g. AAPL, TSLA
                if provided, it will be the direct input to data sources that require a symbol, such as Yahoo Finance.
            rollback_period:
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1w' = 1 week.
            resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
                Default is '1d' = 1 day.
                For convenience, data types such as 'tick', 'second', 'minute' etc. are also supported.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use yesterday's date as the default start date.
            end_date: End date.
                If not specified, use today's date as the end date.
            raw_level:
                'cleaned' (least raw): normalize data (refer to 'normalized' below), also remove all non-standard columns
                    e.g. standard columns in second data are ts, product, open, high, low, close, volume
                'normalized' (default): perform normalization following pfund's convention, preserve all columns
                    Normalization example:
                    - renaming: 'timestamp' -> 'ts'
                    - mapping: 'buy' -> 1, 'sell' -> -1
                'original' (most raw): keep the original data, no transformation will be performed.
                It will be ignored if the data is loaded from storage but not downloaded.
            from_storage: try to load data from this storage.
                If not specified, will search through all storages, e.g. local, minio, cache.
                If no data is found, will try to download the missing data from the data source.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                get_historical_data(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
        """
        assert not self._pipeline_mode, 'get_historical_data() is not supported in pipeline context'
        product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        raw_level = DataRawLevel[raw_level.upper()]
        resolution = Resolution(resolution)
        # make sure the resolution is at least '1d'
        adjusted_resolution = max(resolution, Resolution('1d'))
        # NOTE: all data in storage must be in unit resolution, i.e. no '3m' data in storage
        unit_resolution = Resolution('1' + repr(adjusted_resolution.timeframe))

        # NOTE: since _get_historical_data_from_storage() and _get_historical_data_from_source() both uses etl.resample_data(),
        # the output resolution of df_from_storage and df_from_source must be both = unit_resolution
        df_from_storage, missing_dates = self._get_historical_data_from_storage(
            product,
            unit_resolution,
            start_date,
            end_date,
            raw_level,
            unique_identifier=unique_identifier,
            from_storage=from_storage
        )
        if missing_dates:
            if self.config.print_msg:
                print_warning('''
                Hint:
                    get_historical_data() will first try to load data from local storage (cache, local, minio). 
                    If no data is found, it will download the missing data from the data source and save it to cache.
                    Consider calling download() to download data to your desired storage before calling get_historical_data().
                ''')
            df_from_source: Frame = self._get_historical_data_from_source(
                product,
                unit_resolution,
                missing_dates[0],
                missing_dates[-1],
                raw_level,
                unique_identifier=unique_identifier,
            )
        else:
            df_from_source = None
        
        if df_from_storage is not None and df_from_source is not None:
            df: Frame = nw.concat([df_from_storage, df_from_source])
            df: Frame = df.sort(by='ts', descending=False)
        elif df_from_storage is not None:
            df: Frame = df_from_storage
        elif df_from_source is not None:
            df: Frame = df_from_source
        else:
            df = None
        
        if df is not None:
            is_resample_required = resolution < unit_resolution
            if is_resample_required:
                df: tDataFrame = etl.resample_data(df, resolution)
                self.logger.debug(f'resampled {product.name} {unit_resolution} data to {resolution}')
            else:
                df: tDataFrame = df.to_native()
        return df
