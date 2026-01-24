from __future__ import annotations
from typing import Literal, TYPE_CHECKING, Callable, Awaitable, ClassVar
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from pfund.datas.resolution import Resolution
    from pfund.typing import FullDataChannel
    from pfeed.messaging.streaming_message import StreamingMessage
    from pfeed.data_models.retrieve_market_data_model import RetrieveMarketDataModel
    from pfeed.enums import DataSource
    from pfeed.data_handlers.time_based_data_handler import TimeBasedMetadata
    from pfeed.typing import GenericFrame

import datetime
from pathlib import Path
from abc import abstractmethod
from functools import partial

from pfund.enums import Environment
from pfund.datas.resolution import Resolution
from pfund_kit.style import cprint, RichColor, TextStyle
from pfeed.config import setup_logging, get_config
from pfeed.messaging import BarMessage, TickMessage
from pfeed.enums import MarketDataType, DataLayer, DataTool, StreamMode, ExtractType, DataStorage, IOFormat, Compression, DataCategory
from pfeed.feeds.time_based_feed import TimeBasedFeed
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.requests import MarketFeedDownloadRequest, LoadRequest


config = get_config()


# FIXME: integrate with StreamingFeedMixin
class MarketFeed(TimeBasedFeed):
    data_model_class: ClassVar[type[MarketDataModel]] = MarketDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.MARKET_DATA

    SUPPORTED_LOWEST_RESOLUTION = Resolution('1d')
    
    def get_supported_resolutions(self) -> list[Resolution]:
        market_data_types_or_resolutions: list[str] = self.data_source.generic_metadata['data_categories']['market_data']
        return [Resolution(dtype_or_resol) for dtype_or_resol in market_data_types_or_resolutions]
    
    def get_highest_resolution(self) -> Resolution:
        return sorted(self.get_supported_resolutions(), reverse=True)[0]
    
    def get_lowest_resolution(self) -> Resolution:
        return max(
            sorted(self.get_supported_resolutions(), reverse=False)[0], 
            self.SUPPORTED_LOWEST_RESOLUTION
        )
        
    def get_supported_asset_types(self) -> list[str]:
        from pfund.products.product_basis import ProductAssetType
        market_data_types_or_resolutions: list[str] = self.data_source.generic_metadata['data_categories']['market_data']
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
        env: Environment,
        product: str | BaseProduct,
        resolution: str | Resolution,
        start_date: str | datetime.date,
        end_date: str | datetime.date,
        data_origin: str='',
        **product_specs
    ) -> MarketDataModel:
        if isinstance(product, str) and product:
            product = self.data_source.create_product(product, **product_specs)
        if len(self._dataflows) > 0:
            existing_env = self._dataflows[0].data_model.env
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
    
    def _create_data_model_from_request(self, request: MarketFeedDownloadRequest) -> MarketDataModel:
        return self.create_data_model(
            **request.model_dump(exclude={'product', 'data_resolution', 'target_resolution'}),
            product=request.product,
            resolution=request.target_resolution,
        )
    
    def _create_batch_dataflows(self, extract_func: Callable, extract_type: ExtractType):
        super()._create_batch_dataflows(extract_func=extract_func, extract_type=extract_type)
        # HACK: update the data model in faucet to the data resolution
        # because data model in dataflow uses the target resolution, but the faucet uses the data resolution
        for dataflow in self._dataflows:
            faucet_data_model: MarketDataModel = dataflow.faucet.data_model 
            faucet_data_model.update_resolution(self._current_request.data_resolution)
        
    def download(
        self,
        product: str,
        resolution: Resolution | MarketDataType,
        symbol: str='',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        dataflow_per_date: bool=True,
        to_storage: DataStorage | None=DataStorage.LOCAL,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str = '',
        io_format: IOFormat=IOFormat.PARQUET,
        compression: Compression=Compression.SNAPPY,
        **product_specs
    ) -> GenericFrame | None | MarketFeed:
        '''
        Download historical data from data source.
        
        Args:
            rollback_period: Data resolution or 'ytd' (year to date) or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use rollback_period to determine the start date.
            end_date: End date.
                If not specified, use today's date as the end date.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                download(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
            dataflow_per_date: Whether to create a dataflow for each date.
                If True, a dataflow will be created for each date.
                If False, a single dataflow will be created for the entire date range.
        '''
        env = Environment.BACKTEST
        setup_logging(env=env)
        product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        resolution: Resolution = self.create_resolution(resolution)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)

        # Normalize resolution: use unit resolution as fallback if requested resolution is not supported
        supported_resolutions: list[Resolution] = self.get_supported_resolutions()
        if resolution not in supported_resolutions:
            unit_resolution: Resolution = resolution.to_unit()
            if unit_resolution in supported_resolutions:
                data_resolution: Resolution = unit_resolution
                cprint(
                    f'{resolution} is not supported, using {data_resolution} instead', 
                    style=str(TextStyle.BOLD + RichColor.YELLOW)
                )
            else:
                raise ValueError(f'{resolution} is not supported')
        else:
            data_resolution: Resolution = resolution
        self._validate_resolution_bounds(data_resolution)
        
        self._current_request = MarketFeedDownloadRequest(
            env=env,
            product=product,
            target_resolution=resolution,
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            dataflow_per_date=dataflow_per_date,
        )
        print(f'{self._current_request}')
        exit()
        self._load_request = LoadRequest(
            to_storage=to_storage,
            data_layer=data_layer,
            data_domain=data_domain,
            io_format=io_format,
            compression=compression,
        )
        self._create_batch_dataflows(
            extract_func=lambda data_model: self._download_impl(data_model),
            extract_type=ExtractType.download,
        )
        return self.run() if not self._pipeline_mode else self
    
    def _add_default_transformations_to_download(self):
        '''
        Args:
            data_resolution: The resolution of the downloaded data.
            target_resolution: The resolution of the data to be stored.
        '''
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_to_desired_df
        from pfeed.utils import lambda_with_name
        
        request: MarketFeedDownloadRequest = self._current_request
        is_raw_data = self._load_request.data_layer == DataLayer.RAW

        self.transform(
            lambda_with_name(
                '__convert_to_pandas_df',
                lambda data: convert_to_desired_df(data, DataTool.pandas)
            ),
            lambda_with_name(
                '__standardize_date_column',
                lambda df: self._standardize_date_column(df, is_raw_data)
            ),
        )
        # NOTE: use double underscore to denote default transformations
        if not is_raw_data:
            # default transformations to normalize data:
            self.transform(
                lambda_with_name(
                    '__normalize_raw_data',
                    lambda df: self._normalize_raw_data(df)
                ),
                lambda_with_name(
                    '__standardize_columns',
                    lambda df: etl.standardize_columns(df, request.product, request.data_resolution),
                ),
                lambda_with_name(
                    '__resample_data_if_necessary', 
                    lambda df: etl.resample_data(df, request.target_resolution, product=request.product)
                ),
                # after resampling, the columns order is not guaranteed to be the same as the original, so need to organize them
                # otherwise, polars will not be able to collect correctly
                # resampled_df = organize_columns(resampled_df)
                lambda_with_name(
                    '__organize_columns',
                    lambda df: etl.organize_columns(df)
                ),
            )
        self.transform(
            lambda_with_name(
                '__convert_to_user_df',
                lambda df: convert_to_desired_df(df, config.data_tool)
            )
        )

    def retrieve(
        self,
        product: str,
        resolution: Resolution | MarketDataType,
        rollback_period: str |  Literal['ytd', 'max']="1w",
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str = '',  # TODO
        from_storage: DataStorage=DataStorage.LOCAL,
        auto_resample: bool=True,
        dataflow_per_date: bool=False,
        env: Environment=Environment.BACKTEST,
        **product_specs
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
            from_storage: try to load data from this storage.
                If not specified, will search through all storages, e.g. local, minio, cache.
                If no data is found, will try to download the missing data from the data source.
            auto_resample: Whether to resample data to the target resolution if it is not found in storage.
            dataflow_per_date: Whether to create a dataflow for each date, where data is stored per date.
                If True, a dataflow will be created for each date.
                If False, a single dataflow will be created for the entire date range.
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
        '''
        env = Environment[env.upper()]
        setup_logging(env=env)
        product: BaseProduct = self.create_product(product, **product_specs)
        resolution: Resolution = self.create_resolution(resolution)
        self._validate_resolution_bounds(resolution)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        data_layer = DataLayer[data_layer.upper()]
        return self._run_retrieve(
            partial_dataflow_data_model=partial(self.create_data_model, env=env, product=product, resolution=resolution, data_origin=data_origin),
            partial_faucet_data_model=partial(self.create_data_model, env=env, product=product, resolution=resolution, data_origin=data_origin),
            start_date=start_date,
            end_date=end_date,
            data_layer=data_layer,
            from_storage=from_storage,
            add_default_transformations=lambda: self._add_default_transformations_to_retrieve(resolution, auto_resample),
            dataflow_per_date=dataflow_per_date,
        )
    
    def _retrieve_impl(
        self,
        data_path: Path,
        data_model: RetrieveMarketDataModel,
        data_layer: DataLayer,
        from_storage: DataStorage,
        storage_options: dict | None,
    ) -> tuple[GenericFrame | None, TimeBasedMetadata]:
        '''Retrieve data from storage. If data is not found, search for higher resolutions.
        
        Args:
            data_model: data model passed in by dataflow
        '''
        data_resolution: Resolution = data_model.resolution
        search_resolutions = [data_resolution]
        if data_model.auto_resample:
            highest_resolution: Resolution = self.get_highest_resolution()
            # search for higher resolutions, e.g. search '1m' -> search '1t'
            search_resolutions += [
                resolution for resolution in data_resolution.get_higher_resolutions(exclude_quote=True)
                # remove resolutions that are not supported by the data source
                if resolution <= highest_resolution
            ]
        df: GenericFrame | None = None
        metadata: TimeBasedMetadata = {}
        for search_resolution in search_resolutions:
            data_model_copy = data_model.model_copy(deep=False)
            data_model_copy.update_resolution(search_resolution)
            df, metadata = super()._retrieve_impl(
                data_path=data_path,
                data_model=data_model_copy,
                data_layer=data_layer,
                from_storage=from_storage,
                storage_options=storage_options,
            )
            if df is not None:
                # NOTE: This also updates the faucet's data model with the actual resolution found in storage.
                # This is necessary for auto-resampling: e.g., user requests '1m', but we found '1t' data.
                data_model.update_resolution(search_resolution)
                break
        return df, metadata
    
    def _add_default_transformations_to_retrieve(self, target_resolution: Resolution, auto_resample: bool):
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_to_desired_df
        from pfeed.utils import lambda_with_name

        if auto_resample:
            self.transform(
                lambda_with_name(
                    '__convert_to_pandas_df',
                    lambda df: convert_to_desired_df(df, DataTool.pandas)
                ),
                lambda_with_name(
                    '__resample_data_if_necessary', 
                    lambda df: etl.resample_data(df, target_resolution)
                ),
                lambda_with_name(
                    '__organize_columns',
                    lambda df: etl.organize_columns(df)
                ),
            )
        self.transform(
            lambda_with_name(
                '__convert_to_user_df',
                lambda df: convert_to_desired_df(df, config.data_tool)
            )
        )
    
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
        **product_specs
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
    def _add_default_transformations_to_stream(self, data_layer: DataLayer, product: BaseProduct, resolution: Resolution):
        from pfeed.utils import lambda_with_name
        if data_layer != DataLayer.RAW:
            # NOTE: cannot write self.data_source.name inside self.transform(), otherwise, "self" will be serialized by Ray and return an error
            data_source: DataSource = self.data_source.name
            self.transform(
                lambda_with_name('__standardize_message', lambda msg: MarketFeed._standardize_message(data_source, product, resolution, msg)),
            )
    
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
    
    
    # DEPRECATED
    # def get_historical_data(
    #     self,
    #     product: str,
    #     resolution: Resolution | str | tDataType | Literal['max'],
    #     symbol: str='',
    #     rollback_period: str | Literal['ytd', 'max']="1w",
    #     start_date: str='',
    #     end_date: str='',
    #     data_origin: str='',
    #     data_layer: DataLayer | None=None,
    #     data_domain: str='',
    #     from_storage: DataStorage | None=None,
    #     to_storage: DataStorage | None=None,
    #     storage_options: dict | None=None,
    #     force_download: bool=False,
    #     retrieve_per_date: bool=False,
    #     **product_specs
    # ) -> GenericFrame | None:
    #     from pfeed._etl import market as etl
    #     from pfeed._etl.base import convert_to_pandas_df, convert_to_desired_df

    #     resolution: Resolution = self.create_resolution(resolution)
    #     # handle cases where resolution is less than the minimum resolution, e.g. '3d' -> '1d'
    #     data_resolution: Resolution = max(resolution, self.SUPPORTED_LOWEST_RESOLUTION)
    #     data_domain = data_domain or self.data_domain.value
    #     df: GenericFrame | None = self._get_historical_data_impl(
    #         product=product,
    #         symbol=symbol,
    #         rollback_period=rollback_period,
    #         start_date=start_date,
    #         end_date=end_date,
    #         data_origin=data_origin,
    #         data_layer=data_layer,
    #         data_domain=data_domain,
    #         from_storage=from_storage,
    #         to_storage=to_storage,
    #         storage_options=storage_options,
    #         force_download=force_download,
    #         retrieve_per_date=retrieve_per_date,
    #         product_specs=product_specs,
    #         # NOTE: feed specific kwargs
    #         resolution=data_resolution,
    #     )

    #     # NOTE: df from storage/source should have been resampled, 
    #     # this is only called when resolution is less than the minimum resolution, e.g. '3d' -> '1d'
    #     if df is not None:
    #         is_resample_required = resolution < data_resolution
    #         if is_resample_required:
    #             df: pd.DataFrame = etl.resample_data(convert_to_pandas_df(df), resolution)
    #             self.logger.debug(f'resampled {product} {data_resolution} data to {resolution}')
    #         df: GenericFrame = convert_to_desired_df(df, self._data_tool)
    #     return df