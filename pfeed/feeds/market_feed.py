from __future__ import annotations
from typing import Literal, TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pandas as pd
    from pfund.products.product_base import BaseProduct
    from pfund.datas.resolution import Resolution
    from bytewax.inputs import Source as BytewaxSource
    from bytewax.dataflow import Stream as BytewaxStream
    from pfeed.typing import GenericFrame
    from pfeed.typing import tSTORAGE, tENVIRONMENT, tDATA_LAYER
    from pfeed.data_models.market_data_model import MarketDataModel

import datetime
from functools import partial

from pfeed.feeds.time_based_feed import TimeBasedFeed


tDATA_TYPE = Literal['quote_L3', 'quote_L2', 'quote_L1', 'quote', 'tick', 'second', 'minute', 'hour', 'day']


class MarketFeed(TimeBasedFeed):
    DATA_DOMAIN = 'market_data'
    
    @staticmethod
    def create_resolution(resolution: str | Resolution) -> Resolution:
        from pfund.datas.resolution import Resolution
        return Resolution(resolution) if isinstance(resolution, str) else resolution
    
    SUPPORTED_LOWEST_RESOLUTION = create_resolution('1d')
    
    def create_data_model(
        self,
        product: str | BaseProduct,
        resolution: str | Resolution,
        start_date: str | datetime.date,
        end_date: str | datetime.date | None = None,
        data_origin: str = '',
        env: tENVIRONMENT = 'BACKTEST',
        **product_specs
    ) -> MarketDataModel:
        from pfeed.data_models.market_data_model import MarketDataModel
        if isinstance(product, str) and product:
            product = self.create_product(product, **product_specs)
        if isinstance(start_date, str) and start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str) and end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        return MarketDataModel(
            env=env,
            data_source=self.data_source,
            data_origin=data_origin,
            product=product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date or start_date,
        )
        
    def download(
        self,
        product: str,
        symbol: str='',
        resolution: Resolution | str | tDATA_TYPE | Literal['max']='max',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        to_storage: tSTORAGE='local',
        storage_options: dict | None=None,
        auto_transform: bool=True,
        dataflow_per_date: bool=True,
        include_metadata: bool=False,
        **product_specs
    ) -> GenericFrame | None | tuple[GenericFrame | None, dict[str, Any]] | MarketFeed:
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
            auto_transform: Whether to apply default transformations to the data.
                Default transformations include:
                - normalizing raw data
                - standardizing and organizing columns
                - resampling data to the target resolution
                - filtering non-standard columns if resampling is required
                since during resampling, non-standard columns cannot be resampled/interpreted
            dataflow_per_date: Whether to create a dataflow for each date.
                If True, a dataflow will be created for each date.
                If False, a single dataflow will be created for the entire date range.
        '''
        product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        if resolution == 'max':
            resolution: Resolution = self.data_source.highest_resolution
        else:
            resolution: Resolution = self.create_resolution(resolution)
        assert self.data_source.highest_resolution >= resolution >= self.SUPPORTED_LOWEST_RESOLUTION, \
            f'resolution must be >= {self.SUPPORTED_LOWEST_RESOLUTION} and <= {self.data_source.highest_resolution}'
        # download data must be of unit resolution
        unit_resolution: Resolution = self.create_resolution('1' + repr(resolution.timeframe))
        # allow specified resolution to be lower than the lowest resolution supported by the data source above
        # because that can be handled by resampling, but for the resolution of the actual data to be downloaded, 
        # it must be >= the lowest resolution supported by the data source
        data_resolution: Resolution = max(unit_resolution, self.data_source.lowest_resolution)
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        # if no default and no custom transformations, set data_layer to 'raw'
        if not auto_transform and not self._pipeline_mode and data_layer != 'raw':
            self.logger.debug(f'change data_layer from {data_layer} to "raw" because no default and no custom transformations')
            data_layer = 'raw'
        data_domain = data_domain or self.DATA_DOMAIN
        self.logger.info(f'Downloading historical {unit_resolution} data from {self.name}, from {str(start_date)} to {str(end_date)} (UTC), {data_layer=}/{data_domain=}')
        return self._run_download(
            partial(self.create_data_model, product, data_resolution, data_origin=data_origin),
            start_date, 
            end_date,
            data_layer,
            data_domain,
            to_storage,
            storage_options,
            dataflow_per_date, 
            include_metadata,
            lambda: self._add_default_transformations_to_download(data_resolution, resolution, product) if auto_transform else None,
        )
    
    def _add_default_transformations_to_download(
        self, 
        data_resolution: Resolution,
        target_resolution: Resolution, 
        product: BaseProduct
    ):
        '''
        Args:
            data_resolution: The resolution of the downloaded data.
            target_resolution: The resolution of the data to be stored.
        '''
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_to_pandas_df, convert_to_user_df
        from pfeed.utils.utils import lambda_with_name

        self.transform(
            convert_to_pandas_df,
            self._normalize_raw_data,
            lambda_with_name(
                'standardize_columns',
                lambda df: etl.standardize_columns(df, data_resolution, product.name, symbol=product.symbol),
            ),
            etl.filter_columns,
            lambda_with_name(
                'resample_data_if_necessary', 
                lambda df: etl.resample_data(df, target_resolution)
            ),
            etl.organize_columns,
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_user_df(df, self.data_tool.name)
            )
        )

    def retrieve(
        self,
        product: str,
        resolution: Resolution | str | tDATA_TYPE,
        rollback_period: str="1w",
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        from_storage: tSTORAGE | None=None,
        storage_options: dict | None=None,
        auto_transform: bool=True,
        dataflow_per_date: bool=False,
        include_metadata: bool=False,
        **product_specs
    ) -> GenericFrame | None | tuple[GenericFrame | None, dict[str, Any]] | MarketFeed:
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
            data_layer:
                'curated' (least raw): normalize data (refer to 'cleaned' below), also remove all non-standard columns
                    e.g. standard columns in second data are ts, product, open, high, low, close, volume
                'cleaned' (default): perform normalization following pfund's convention, preserve all columns
                    Normalization example:
                    - renaming: 'timestamp' -> 'date'
                    - mapping: 'buy' -> 1, 'sell' -> -1
                'raw' (most raw): keep the original data, no transformation will be performed.
                It will be ignored if the data is loaded from storage but not downloaded.
            data_domain: The domain of the data. e.g. 'market_data'. Can be a custom domain.
            from_storage: try to load data from this storage.
                If not specified, will search through all storages, e.g. local, minio, cache.
                If no data is found, will try to download the missing data from the data source.
            auto_transform: Whether to apply default transformations to the data.
                Default transformations include:
                - resampling data to the target resolution
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
        '''
        product: BaseProduct = self.create_product(product, **product_specs)
        resolution: Resolution = self.create_resolution(resolution)
        assert resolution >= self.SUPPORTED_LOWEST_RESOLUTION, f'resolution must be >= minimum resolution {self.SUPPORTED_LOWEST_RESOLUTION}'
        unit_resolution: Resolution = self.create_resolution('1' + repr(resolution.timeframe))
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        data_domain = data_domain or self.DATA_DOMAIN
        self.logger.info(f'Retrieving {self.name} {resolution} data {from_storage=}, from {str(start_date)} to {str(end_date)} (UTC), {data_layer=}/{data_domain=}')
        return self._run_retrieve(
            partial(self.create_data_model, product, unit_resolution, data_origin=data_origin),
            start_date,
            end_date,
            data_layer,
            data_domain,
            from_storage,
            storage_options,
            lambda: self._add_default_transformations_to_retrieve(resolution) if auto_transform else None,
            dataflow_per_date,
            include_metadata,
        )
    
    def _retrieve_impl(
        self,
        data_model: MarketDataModel,
        data_layer: tDATA_LAYER,
        data_domain: str,
        from_storage: tSTORAGE | None=None,
        storage_options: dict | None=None,
    ) -> tuple[GenericFrame | None, dict[str, Any]]:
        '''Retrieve data from storage.
        If data is not found, search for higher resolutions.
        NOTE: This will change the resolution of the data model.
        '''
        # if can't find unit_resolution in storage, search for higher resolutions
        # e.g. search '1m' -> search '1t'
        unit_resolution = data_model.resolution
        assert unit_resolution.period == 1, 'unit_resolution must have period = 1'
        search_resolutions = [unit_resolution] + [
            resolution for resolution in unit_resolution.get_higher_resolutions(exclude_quote=True) 
            # remove resolutions that are not supported by the data source
            if resolution <= self.data_source.highest_resolution
        ]
        data: GenericFrame | None = None
        metadata: dict[str, list[datetime.date]] = {}
        for search_resolution in search_resolutions:
            data_model.update_resolution(search_resolution)
            data, metadata = super()._retrieve_impl(
                data_model,
                data_layer,
                data_domain,
                from_storage=from_storage,
                storage_options=storage_options,
            )
            if data is not None:
                break
        return data, metadata
    
    def _add_default_transformations_to_retrieve(self, target_resolution: Resolution):
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_to_pandas_df, convert_to_user_df
        from pfeed.utils.utils import lambda_with_name

        self.transform(
            convert_to_pandas_df,
            lambda_with_name(
                'resample_data_if_necessary', 
                lambda df: etl.resample_data(df, target_resolution)
            ),
            etl.organize_columns,
            lambda_with_name(
                'convert_to_user_df',
                lambda df: convert_to_user_df(df, self.data_tool.name)
            )
        )
    
    def get_historical_data(
        self,
        product: str,
        resolution: Resolution | str | tDATA_TYPE | Literal['max'],
        symbol: str='',
        rollback_period: str | Literal['ytd', 'max']="1w",
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        from_storage: tSTORAGE | None=None,
        to_storage: tSTORAGE='cache',
        storage_options: dict | None=None,
        force_download: bool=False,
        **product_specs
    ) -> GenericFrame | None:
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_to_pandas_df, convert_to_user_df

        resolution: Resolution = self.create_resolution(resolution)
        # handle cases where resolution is less than the minimum resolution, e.g. '3d' -> '1d'
        data_resolution: Resolution = max(resolution, self.SUPPORTED_LOWEST_RESOLUTION)

        df: GenericFrame | None = self._get_historical_data_impl(
            product=product,
            symbol=symbol,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            data_layer=data_layer,
            data_domain=data_domain,
            from_storage=from_storage,
            to_storage=to_storage,
            storage_options=storage_options,
            force_download=force_download,
            product_specs=product_specs,
            # NOTE: feed specific kwargs
            resolution=data_resolution,
        )
        
        # NOTE: df from storage/source should have been resampled, 
        # this is only called when resolution is less than the minimum resolution, e.g. '3d' -> '1d'
        if df is not None:
            is_resample_required = resolution < data_resolution
            if is_resample_required:
                df: pd.DataFrame = etl.resample_data(convert_to_pandas_df(df), resolution)
                self.logger.debug(f'resampled {product.name} {data_resolution} data to {resolution}')
            df: GenericFrame = convert_to_user_df(df, self.data_tool.name)
        return df
    
    # TODO
    def stream(
        self,
        products: list[str],
        channel: str,
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
        auto_transform: bool=True,
    ) -> MarketFeed:
        # dataflow: DataFlow = self._create_stream_dataflow(bytewax_source=bytewax_source)
        # if auto_transform:
        #     self._add_default_transformations_to_stream()
        raise NotImplementedError(f"{self.name} stream() is not implemented")
        
    # TODO
    def _add_default_transformations_to_stream(self):
        pass
    
    # TODO: General-purpose data fetching/LLM call? without storage overhead
    def fetch(self) -> GenericFrame | None | MarketFeed:
        raise NotImplementedError(f"{self.name} fetch() is not implemented")
    
    # TODO
    def get_realtime_data(self) -> GenericFrame | None:
        assert not self._pipeline_mode, 'pipeline mode is not supported in get_realtime_data()'
        self.fetch()
