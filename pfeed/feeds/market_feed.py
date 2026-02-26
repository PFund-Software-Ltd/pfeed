# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportArgumentType=false
from __future__ import annotations
from typing import Literal, TYPE_CHECKING, Callable, ClassVar, Any, cast
if TYPE_CHECKING:
    from collections.abc import Awaitable
    import pandas as pd
    from pfund.entities.products.product_base import BaseProduct
    from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
    from pfeed.requests import MarketFeedDownloadRequest, MarketFeedRetrieveRequest
    from pfund.datas.resolution import Resolution
    from pfund.typing import FullDataChannel
    from pfeed.streaming.streaming_message import StreamingMessage
    from pfeed.enums import DataSource
    from pfeed.data_handlers.time_based_data_handler import TimeBasedMetadata
    from pfeed.typing import GenericFrame

import datetime
from abc import ABC, abstractmethod

from pfund.enums import Environment
from pfund.datas.resolution import Resolution
from pfund_kit.style import RichColor, TextStyle, cprint
from pfeed.config import setup_logging, get_config
from pfeed.streaming import BarMessage, TickMessage
from pfeed.enums import MarketDataType, DataLayer, DataTool, StreamMode, DataStorage, DataCategory
from pfeed.feeds.time_based_feed import TimeBasedFeed
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.storages.storage_config import StorageConfig
from pfeed.storages.base_storage import BaseStorage


config = get_config()


# FIXME: integrate with StreamingFeedMixin
class MarketFeed(TimeBasedFeed, ABC):
    data_model_class: ClassVar[type[MarketDataModel]] = MarketDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.MARKET_DATA
    SUPPORTED_LOWEST_RESOLUTION: ClassVar[Resolution] = Resolution('1d')

    _current_request: MarketFeedBaseRequest

    @staticmethod
    @abstractmethod
    def _normalize_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    def get_supported_resolutions(self) -> list[Resolution]:
        market_data_types_or_resolutions: list[str] = self.data_source.generic_metadata['data_categories']['market_data']
        return [Resolution(dtype_or_resol) for dtype_or_resol in market_data_types_or_resolutions]
    
    def get_highest_resolution(self) -> Resolution:
        return sorted(self.get_supported_resolutions(), reverse=True)[0]
    
    def get_lowest_resolution(self) -> Resolution:
        return min(
            sorted(self.get_supported_resolutions(), reverse=False)[0], 
            self.SUPPORTED_LOWEST_RESOLUTION
        )
        
    def get_supported_asset_types(self) -> list[str]:
        from pfund.entities.products.product_basis import ProductAssetType
        market_data_types_or_resolutions: dict[str, list[str]] = self.data_source.generic_metadata['data_categories']['market_data']
        return list(set(
            str(ProductAssetType(as_string=asset_type.upper()))
            for dtype_or_resol in market_data_types_or_resolutions
            for asset_type in market_data_types_or_resolutions[dtype_or_resol]
        ))
    
    @staticmethod
    def create_resolution(resolution: str | Resolution) -> Resolution:
        return Resolution(resolution) if isinstance(resolution, str) else resolution
            
    def _validate_resolution_bounds(self, resolution: Resolution):
        highest_resolution: Resolution = self.get_highest_resolution()
        lowest_resolution: Resolution = self.get_lowest_resolution()
        assert highest_resolution >= resolution >= lowest_resolution, \
            f'resolution must be >= {lowest_resolution} and <= {highest_resolution}'
    
    def create_data_model(
        self,
        product: str | BaseProduct,
        resolution: str | Resolution,
        start_date: str | datetime.date,
        end_date: str | datetime.date = '',
        env: Environment = Environment.BACKTEST,
        data_origin: str='',
        **product_specs: Any
    ) -> MarketDataModel:
        '''Create a MarketDataModel instance.

        Args:
            product: product basis (e.g. 'BTC_USDT_PERP') or a Product instance.
            resolution: Data resolution string (e.g. '1m', '1h') or a Resolution instance.
            start_date: Start date as a string ('YYYY-MM-DD') or datetime.date.
            end_date: End date as a string ('YYYY-MM-DD') or datetime.date.
                If not provided (empty string ''), defaults to start_date, creating a single-day model.
            env: Trading environment.
            data_origin: Origin label for the data.
            product_specs: Additional product specifications (e.g. strike_price, expiration for options).
        '''
        if isinstance(product, str):
            if not product:
                raise ValueError('product must be a non-empty string or a BaseProduct instance')
            product = self.data_source.create_product(product, **product_specs)
        if len(self._dataflows) > 0:
            existing_env = cast(MarketDataModel, self._dataflows[0].data_model).env
            if existing_env != env:
                raise ValueError(f'{self.name} dataflows have different environments: {existing_env} and {env}')
        DataModel = self.data_model_class
        return DataModel(
            env=env,
            data_source=self.data_source,
            data_origin=data_origin,
            product=product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date or start_date,
        )
    
    def _create_data_model_from_request(self, request: MarketFeedBaseRequest) -> MarketDataModel:
        return self.create_data_model(
            **request.model_dump(exclude={'extract_type', 'dataflow_per_date', 'product', 'data_resolution'}),
            product=request.product,
            resolution=request.target_resolution,
        )
    
    def download(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        symbol: str='',
        rollback_period: Resolution | str | Literal['ytd', 'max']='1d',
        start_date: datetime.date | str='',
        end_date: datetime.date | str='',
        data_origin: str='',
        dataflow_per_date: bool=True,
        clean_raw_data: bool=True,
        storage_config: StorageConfig | None=None,
        **product_specs: Any
    ) -> GenericFrame | None | MarketFeed:
        '''Download historical data from data source.

        Args:
            product: product basis, e.g. BTC_USDT_PERP, AAPL_USD_STK.
                Details of specifications should be specified in `product_specs`.
            resolution: Data resolution, e.g. '1m', '1h', 'day'.
            symbol: Symbol used by the data source. If not specified, derived from `product`.
                Note that the derived symbol might NOT be correct, in that case, you should specify it manually.
            rollback_period: Period to rollback from today, only used when `start_date` is not specified.
                Accepts a resolution string, 'ytd' (year to date), or 'max'. Default is '1d'.
            start_date: Start date.
                If not specified, rollback_period is used to determine the start date.
                Special case: if rollback_period='max', uses the data source's start_date attribute.
            end_date: End date. If not specified, use today's date.
            data_origin: Origin label for the data, used to distinguish data from different sources.
            dataflow_per_date: Whether to create a dataflow for each date.
                If True, a dataflow will be created for each date.
                If False, a single dataflow will be created for the entire date range.
            clean_raw_data: Whether to clean raw data after download.
                If storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
                If True, downloaded raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
                If False, downloaded raw data will be returned as is.
            storage_config: Storage configuration.
                if None, downloaded data will NOT be stored to storage.
                if provided, downloaded data will be stored to storage based on the storage config.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                download(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.

        Returns:
            Downloaded data as a DataFrame, or None if no data is available.
            Returns self if used in pipeline mode (i.e. after calling `.pipeline()`).
        '''
        from pfeed.requests import MarketFeedDownloadRequest

        assert any([start_date, end_date, rollback_period]), 'at least one of start_date, end_date, or rollback_period must be provided'
        env = Environment.BACKTEST
        setup_logging(env=env)
        product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        resolution: Resolution = self.create_resolution(resolution)
        # find the first resolution that is greater than or equal to the target resolution
        for _resolution in sorted(self.get_supported_resolutions(), reverse=False):
            if _resolution >= resolution:
                data_resolution = _resolution
                break
        else:
            raise ValueError(f'{resolution} is not supported')
        if data_resolution != resolution:
            self.logger.warning(
                f'{resolution=} is not supported, download data with resolution={data_resolution} instead', 
            )
        self._validate_resolution_bounds(data_resolution)
        self._current_request = MarketFeedDownloadRequest(
            storage_config=storage_config,
            env=env,
            product=product,
            target_resolution=resolution,
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            dataflow_per_date=dataflow_per_date,
            clean_raw_data=storage_config.data_layer != DataLayer.RAW if storage_config else clean_raw_data,
        )
        self.logger.info(
            f'{self._current_request.name}:\n{self._current_request}\n', 
            style=TextStyle.BOLD + RichColor.GREEN  # pyright: ignore[reportCallIssue]
        )
        self._create_batch_dataflows(extract_func=self._download_impl)
        # NOTE: update the data model in faucet to the data resolution
        # because data model in dataflow uses the target resolution, but the faucet uses the data resolution
        for dataflow in self._dataflows:
            faucet_data_model: MarketDataModel = cast(MarketDataModel, dataflow.faucet.data_model)
            faucet_data_model.update_resolution(self._current_request.data_resolution)
        return self.run() if not self.is_pipeline() else self
    
    def _get_default_transformations_for_download(self) -> list[Callable[..., Any]]:
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_to_desired_df
        from pfeed.utils import lambda_with_name
        from pfeed.requests import MarketFeedDownloadRequest, MarketFeedRetrieveRequest
        
        request: MarketFeedDownloadRequest | MarketFeedRetrieveRequest = cast(
            MarketFeedDownloadRequest | MarketFeedRetrieveRequest, 
            self._current_request
        )

        if isinstance(request, MarketFeedDownloadRequest):
            clean_raw_data = request.clean_raw_data
        elif isinstance(request, MarketFeedRetrieveRequest):
            clean_raw_data = request.clean_raw_data
        else:
            raise ValueError(f'unknown request type: {type(request)}')

        default_transformations = [
            lambda_with_name(
                'convert_to_pandas_df',
                lambda data: convert_to_desired_df(data, DataTool.pandas)
            ),
            lambda_with_name(
                'standardize_date_column',
                lambda df: self._standardize_date_column(df, is_raw_data=not clean_raw_data)
            ),
        ]
        if clean_raw_data:
            default_transformations.extend([
                self._normalize_raw_data,
                lambda_with_name(
                    'standardize_columns',
                    lambda df: etl.standardize_columns(df, request.product, request.data_resolution),
                ),
                lambda_with_name(
                    'resample_data_if_necessary',
                    lambda df: etl.resample_data(df, request.target_resolution, product=request.product)
                ),
                etl.organize_columns
            ])
        default_transformations.append(
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_desired_df(df, config.data_tool)
            ),
        )
        return default_transformations

    def _find_stored_resolution(
        self,
        storage: BaseStorage,
        data_model: MarketDataModel,
        search_resolutions: list[Resolution],
        probe_days: int = 7,
    ) -> Resolution | None:
        """Probe storage to find which resolution the data is stored at, without reading data.

        Args:
            probe_days: Number of days from the first date to probe. Defaults to 7.

        Returns:
            The resolution at which data was found in storage, or None if no data exists.
        """
        first_date = data_model.dates[0]
        last_date = data_model.dates[-1]
        probe_end_date = min(first_date + datetime.timedelta(days=probe_days - 1), last_date)
        probe_data_model = data_model.model_copy(deep=False)
        probe_data_model.update_start_date(first_date)
        probe_data_model.update_end_date(probe_end_date)

        for resolution in search_resolutions:
            probe_copy = probe_data_model.model_copy(deep=False)
            probe_copy.update_resolution(resolution)
            _ = storage.with_data_model(probe_copy)
            metadata = storage.data_handler.read_metadata()
            # if this date's source path exists, data is stored at this resolution
            if not metadata.missing_source_paths:
                return resolution
        else:
            self.logger.warning(f'failed to find data {probe_data_model} in {storage}')
        return None

    def retrieve(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        symbol: str='',
        rollback_period: str |  Literal['ytd', 'max']="1d",
        start_date: datetime.date | str='',
        end_date: datetime.date | str='',
        data_origin: str='',
        env: Environment=Environment.BACKTEST,
        dataflow_per_date: bool | None=None,
        clean_raw_data: bool=False,
        storage_config: StorageConfig | None=None,
        **product_specs: Any
    ) -> GenericFrame | None | MarketFeed:
        '''Retrieve data from storage.
        Args:
            product: Financial product, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
                For convenience, data types such as 'tick', 'second', 'minute' etc. are also supported.
            rollback_period:
                Period to rollback from today, only used when `start_date` is not specified.
                Default is '1w' = 1 week.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use rollback_period to determine the start date.
            end_date: End date.
                If not specified, use today's date as the end date.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                retrieve(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
            env: Environment to retrieve data from.
            dataflow_per_date: Whether to create a dataflow for each date.
                If None (default), automatically determined by probing storage:
                    True if stored data needs resampling (stored resolution != target resolution),
                    False if no resampling needed (single scan_parquet is more efficient).
                If True/False, uses the specified value directly.
            clean_raw_data: Whether to clean raw data.
                If data_layer is not RAW in storage_config, this parameter will be ignored.
                If True, raw data stored in data layer=RAW will be cleaned using the default transformations for download.
                If False, raw data stored in data layer=RAW will be loaded as is.
        '''
        from pfeed.requests import MarketFeedRetrieveRequest

        assert any([start_date, end_date, rollback_period]), 'at least one of start_date, end_date, or rollback_period must be provided'
        env = Environment[env.upper()]
        setup_logging(env=env)
        product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        resolution: Resolution = self.create_resolution(resolution)
        # search for higher resolutions (highest first), e.g. if resolution is '1m', search '1m' -> '1t' -> '1s'
        search_resolutions = [resolution] + sorted(
            [_resolution for _resolution in self.get_supported_resolutions() if _resolution > resolution],
            reverse=True,
        )
        for _resolution in search_resolutions:
            self._validate_resolution_bounds(_resolution)
        storage_config = storage_config or StorageConfig()
        # NOTE: Create storage in main thread to avoid Ray process config loss.
        # Ray workers reload config via get_config(), losing pe.configure(data_path=...) changes.
        Storage = cast(type[BaseStorage], storage_config.storage.storage_class)  # pyright: ignore[reportAttributeAccessIssue]
        storage = (
            Storage(
                data_path=storage_config.data_path,
                data_layer=storage_config.data_layer,
                data_domain=storage_config.data_domain or self.data_domain.value,
                storage_options=self._storage_options.get(storage_config.storage, {}),  # pyright: ignore[reportCallIssue]
            )
            .with_io(
                io_options=self._io_options.get(storage_config.io_format, {}),  # pyright: ignore[reportCallIssue]
                io_format=storage_config.io_format,
                compression=storage_config.compression,
            )
        )
        # Auto-determine dataflow_per_date if not explicitly set
        probe_data_model = self.create_data_model(
            env=env,
            product=product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
        )
        stored_resolution = self._find_stored_resolution(storage, probe_data_model, search_resolutions)
        # resampling needed → per-date dataflows for parallel Ray tasks
        # no resampling (or no data found) → single dataflow with one scan_parquet
        if dataflow_per_date is None:
            dataflow_per_date = stored_resolution is not None and stored_resolution != resolution
            if dataflow_per_date and self._num_batch_workers is None:
                cprint(
                    'dataflow_per_date is True but num_batch_workers is None, Ray is NOT being used, retrieving data will be done sequentially', 
                    style=TextStyle.BOLD + RichColor.YELLOW
                )
        self._current_request = MarketFeedRetrieveRequest(
            storage_config=storage_config,
            env=env,
            product=product,
            target_resolution=resolution,
            data_resolution=stored_resolution if stored_resolution else resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            dataflow_per_date=dataflow_per_date,
            clean_raw_data=clean_raw_data,
        )
        self.logger.info(
            f'{self._current_request.name}:\n{self._current_request}\n',
            style=TextStyle.BOLD + RichColor.GREEN  # pyright: ignore[reportCallIssue]
        )
        self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(
                data_model=data_model,
                storage=storage,
                stored_resolution=stored_resolution,
            ),
        )
        return self.run() if not self.is_pipeline() else self
    
    def _retrieve_impl(
        self,
        data_model: MarketDataModel,
        storage: BaseStorage,
        stored_resolution: Resolution | None,
    ) -> tuple[GenericFrame | None, TimeBasedMetadata | None]:
        df: GenericFrame | None = None
        metadata: TimeBasedMetadata | None = None
        if stored_resolution is not None:
            data_model_copy = data_model.model_copy(deep=False)
            data_model_copy.update_resolution(stored_resolution)
            storage = storage.with_data_model(data_model_copy)
            df, _metadata = storage.read_data(include_metadata=True)
            metadata = cast(TimeBasedMetadata | None, _metadata)
            if df is not None:
                self.logger.debug(f'found data {data_model_copy} in {storage}')
                # update storage's data model back to the original data model
                _ = storage.with_data_model(data_model)
        return df, metadata
    
    def _get_default_transformations_for_retrieve(self) -> list[Callable[..., Any]]:
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_to_desired_df
        from pfeed.utils import lambda_with_name

        request: MarketFeedRetrieveRequest = cast(MarketFeedRetrieveRequest, self._current_request)
        if not request.clean_raw_data:
            default_transformations = [
                lambda_with_name(
                    'convert_to_pandas_df',
                    lambda df: convert_to_desired_df(df, DataTool.pandas)
                ),
                lambda_with_name(
                    'resample_data_if_necessary', 
                    lambda df: etl.resample_data(df, request.target_resolution)
                ),
                etl.organize_columns,
                lambda_with_name(
                    'convert_to_user_df',
                    lambda df: convert_to_desired_df(df, config.data_tool)
                )
            ]
        else:
            default_transformations = self._get_default_transformations_for_download()
        return default_transformations
    
    def stream(
        self,
        product: str,
        symbol: str='',
        resolution: Resolution | MarketDataType=MarketDataType.TICK,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_origin: str='',
        to_storage: DataStorage=DataStorage.LOCAL,
        callback: Callable[[dict], Awaitable[None] | None] | None=None,
        env: Environment=Environment.LIVE,
        mode: StreamMode=StreamMode.FAST,
        flush_interval: int=100,  # in seconds
        **product_specs: Any
    ) -> MarketFeed:
        '''
        Args:
            stream_mode: SAFE or FAST
                if "FAST" is chosen, streaming data will be cached to memory to a certain amount before writing to disk,
                faster write speed, but data loss risk will increase.
                if "SAFE" is chosen, streaming data will be written to disk immediately,
                slower write speed, but data loss risk will be minimized.
            flush_interval: Interval in seconds for flushing buffered streaming data to storage. Default is 100 seconds.
                If using deltalake:
                Frequent flushes will reduce write performance and generate many small files 
                (e.g. part-00001-0a1fd07c-9479-4a72-8a1e-6aa033456ce3-c000.snappy.parquet).
                Infrequent flushes create larger files but increase data loss risk during crashes when using FAST stream_mode.
                This is expected to be fine-tuned based on the actual use case.
        '''
        from pfund_kit.utils.temporal import get_utc_now
        
        env = Environment[env.upper()]
        assert env != Environment.BACKTEST, 'streaming is not supported in env BACKTEST'
        setup_logging(env=env)
        product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        resolution: Resolution = self.create_resolution(resolution)
        data_layer = DataLayer[data_layer.upper()]
        data_model: MarketDataModel = self.create_data_model(
            env=env,
            product=product,
            resolution=resolution,
            data_origin=data_origin,
            start_date=get_utc_now().date(),
        )
        self._add_data_channel(data_model)
        self._create_streaming_settings(mode=mode, flush_interval=flush_interval)
        return self._run_stream(
            data_model=data_model,
            add_default_transformations=lambda: self._add_default_transformations_to_stream(data_layer, product, resolution),
            load_to_storage=(lambda: self.load(to_storage, data_layer)) if to_storage else None,
            callback=callback,
        )
    
    # NOTE: ALL transformation functions MUST be static methods so that they can be serialized by Ray
    def _get_default_transformations_for_stream(self, data_layer: DataLayer, product: BaseProduct, resolution: Resolution):
        from pfeed.utils import lambda_with_name
        default_transformations = []
        if data_layer != DataLayer.RAW:
            # NOTE: cannot write self.data_source.name inside self.transform(), otherwise, "self" will be serialized by Ray and return an error
            data_source: DataSource = self.data_source.name
            default_transformations.append(
                lambda_with_name('standardize_message', lambda msg: MarketFeed._standardize_message(data_source, product, resolution, msg)),
            )
        return default_transformations
    
    @staticmethod
    def _standardize_message(data_source: DataSource, product: BaseProduct, resolution: Resolution, msg: dict) -> StreamingMessage:
        if resolution.is_tick():
            data = msg['data']
            message = TickMessage(
                data_source=data_source.value,
                product=product.name,
                basis=str(product.basis),
                symbol=product.symbol,
                specs=product.specs,
                resolution=repr(resolution),
                ts=data['ts'],
                price=data['price'],
                volume=data['volume'],
                extra_data=msg.get('extra_data', {}),
            )
        elif resolution.is_bar():
            data: dict= msg['data']
            message = BarMessage(
                data_source=data_source.value,
                product=product.name,
                basis=str(product.basis),
                symbol=product.symbol,
                specs=product.specs,
                resolution=repr(resolution),
                msg_ts=msg['ts'],
                ts=data['ts'],
                open=data['open'],
                high=data['high'],
                low=data['low'],
                close=data['close'],
                volume=data['volume'],
                extra_data=msg.get('extra_data', {}),
                is_incremental=msg['is_incremental'],
            )
        else:
            raise NotImplementedError(f'{product.symbol} {resolution} is not supported')
        return message
    
    @staticmethod
    @abstractmethod
    def _parse_message(product: BaseProduct, msg: dict) -> dict:
        pass
    
    @abstractmethod
    def _add_data_channel(self, data_model: MarketDataModel) -> FullDataChannel:
        pass
    
    @abstractmethod
    def add_channel(self, channel: FullDataChannel, channel_type: Literal['public', 'private'], *args, **kwargs):
        '''
        Args:
            channel: A channel to subscribe to.
                The channel specified will be used directly for the external data source, no modification will be made.
                Useful for subscribing channels not related to resolution.
        '''
        pass
        
    # TODO: General-purpose data fetching/LLM call? without storage overhead
    # NOTE: integrate it well with prefect's serve()
    def fetch(self) -> GenericFrame | None | MarketFeed:
        raise NotImplementedError(f"{self.name} fetch() is not implemented")
