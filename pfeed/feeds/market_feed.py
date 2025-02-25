from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from narwhals.typing import Frame
    from pfund.products.product_base import BaseProduct
    from bytewax.inputs import Source as BytewaxSource
    from bytewax.dataflow import Stream as BytewaxStream
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tSTORAGE, tENVIRONMENT, tDATA_LAYER
    from pfeed.flows.dataflow import DataFlow

import datetime

import pandas as pd
import narwhals as nw
from rich.console import Console

from pfund.datas.resolution import Resolution
from pfeed._etl import market as etl
from pfeed._etl.base import convert_to_pandas_df, convert_to_user_df
from pfeed.feeds.base_feed import BaseFeed, clear_subflows
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.const.enums import DataAccessType
from pfeed.utils.utils import lambda_with_name
from pfeed.utils.dataframe import is_empty_dataframe


tDATA_TYPE = Literal['quote_L3', 'quote_L2', 'quote_L1', 'quote', 'tick', 'second', 'minute', 'hour', 'day']


class MarketFeed(BaseFeed):
    DATA_DOMAIN = 'market_data'
    
    @property
    def global_min_resolution(self) -> Resolution:
        return Resolution('1d')
    
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
        
    @clear_subflows
    def download(
        self,
        product: str,
        symbol: str='',
        resolution: Resolution | str | tDATA_TYPE='1m',
        rollback_period: str | Literal['ytd', 'max']='1d',
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        to_storage: tSTORAGE='local',
        storage_configs: dict | None=None,
        auto_transform: bool=True,
        concat_output: bool=True,
        **product_specs
    ) -> tDataFrame | None | dict[datetime.date, tDataFrame | None] | MarketFeed:
        '''
        Download historical data from data source.
        
        Args:
            rollback_period: Data resolution or 'ytd' (year to date) or 'max'
                Period to rollback from today, only used when `start_date` is not specified.
            start_date: Start date.
                If not specified:
                    If the data source has a 'start_date' attribute, use it as the start date.
                    Otherwise, use yesterday's date as the default start date.
            end_date: End date.
                If not specified, use today's date as the end date.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                get_historical_data(
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
            concat_output: Whether to concatenate the data from different dates.
                If True, the data from different dates will be concatenated into a single DataFrame.
                If False, the data from different dates will be returned as a dictionary of DataFrames with date as the key.
        '''
        product: BaseProduct = self.create_product(product, symbol=symbol, **product_specs)
        resolution = Resolution(resolution) if isinstance(resolution, str) else resolution
        assert resolution >= self.global_min_resolution, f'resolution must be >= minimum resolution {self.global_min_resolution}'
        unit_resolution = Resolution('1' + repr(resolution.timeframe))
        adjusted_resolution = min(
            self.data_source.highest_resolution,
            max(unit_resolution, self.data_source.lowest_resolution)
        )
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        # if no default and no custom transformations, set data_layer to 'raw'
        if not auto_transform and not self._pipeline_mode:
            data_layer = 'raw'
        if start_date and end_date:
            Console().print(f'Downloading historical {resolution} data from {self.name}, from {str(start_date)} to {str(end_date)} (UTC), {data_layer=}', style='bold yellow')
        self._create_download_dataflows(
            product,
            resolution,
            start_date,
            end_date,
            data_origin=data_origin,
        )
        if auto_transform:
            self._add_default_transformations_to_download(adjusted_resolution, resolution, product)
        return self._run_download(
            data_layer=data_layer,
            data_domain=data_domain or self.DATA_DOMAIN,
            to_storage=to_storage,
            storage_configs=storage_configs,
            concat_output=concat_output,
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

    @clear_subflows
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
        storage_configs: dict | None=None,
        auto_transform: bool=True,
        concat_output: bool=True,
        **product_specs
    ) -> tDataFrame | None | dict[datetime.date, tDataFrame | None] | MarketFeed:
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
                    Otherwise, use yesterday's date as the default start date.
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
            concat_output: Whether to concatenate the data from different dates.
                If True, the data from different dates will be concatenated into a single DataFrame.
                If False, the data from different dates will be returned as a dictionary of DataFrames with date as the key.
            product_specs: The specifications for the product.
                if product is "BTC_USDT_OPT", you need to provide the specifications of the option as kwargs:
                get_historical_data(
                    product='BTC_USDT_OPT',
                    strike_price=10000,
                    expiration='2024-01-01',
                    option_type='CALL',
                )
                The most straight forward way to know what attributes to specify is leave it empty and read the exception message.
        '''
        product: BaseProduct = self.create_product(product, **product_specs)
        resolution = Resolution(resolution) if isinstance(resolution, str) else resolution
        assert resolution >= self.global_min_resolution, f'resolution must be >= minimum resolution {self.global_min_resolution}'
        unit_resolution = Resolution('1' + repr(resolution.timeframe))
        start_date, end_date = self._standardize_dates(start_date, end_date, rollback_period)
        self._create_retrieve_dataflows(
            product,
            unit_resolution,
            start_date,
            end_date,
            data_origin,
            data_layer,
            data_domain or self.DATA_DOMAIN,
            from_storage=from_storage,
            storage_configs=storage_configs,
        )
        if auto_transform:
            self._add_default_transformations_to_retrieve(resolution)
        return self._run_retrieve(concat_output=concat_output)
    
    def _execute_retrieve(
        self,
        data_model: MarketDataModel,
        data_layer: tDATA_LAYER,
        data_domain: str,
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
    ) -> tDataFrame | None:
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
        data = None
        for search_resolution in search_resolutions:
            data_model.update_resolution(search_resolution)
            data: tDataFrame | None = super()._execute_retrieve(
                data_model,
                data_layer,
                data_domain,
                from_storage=from_storage,
                storage_configs=storage_configs,
            )
            if data is not None:
                break
        return data
        
    def _create_retrieve_dataflows(
        self,
        product: BaseProduct,
        unit_resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        data_origin: str,
        data_layer: tDATA_LAYER,
        data_domain: str,
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
    ) -> list[DataFlow]:
        assert unit_resolution.period == 1, 'unit_resolution must have period = 1'
        dataflows: list[DataFlow] = []
        # NOTE: one data model per date
        for date in pd.date_range(start_date, end_date).date:
            data_model = self.create_data_model(product, unit_resolution, date, data_origin=data_origin)
            dataflow: DataFlow = self._extract_retrieve(
                data_model,
                data_layer,
                data_domain,
                from_storage=from_storage,
                storage_configs=storage_configs,
            )
            dataflows.append(dataflow)
        return dataflows
    
    def _add_default_transformations_to_retrieve(self, target_resolution: Resolution):
        self.transform(
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
        resolution: Resolution | str | tDATA_TYPE,
        symbol: str='',
        rollback_period: str | Literal['ytd', 'max']="1w",
        start_date: str='',
        end_date: str='',
        data_origin: str='',
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='',
        from_storage: tSTORAGE | None=None,
        storage_configs: dict | None=None,
        **product_specs
    ) -> tDataFrame | None:
        assert not self._pipeline_mode, 'pipeline mode is not supported in get_historical_data()'
        resolution = Resolution(resolution) if isinstance(resolution, str) else resolution
        # handle cases where resolution is less than the minimum resolution, e.g. '3d' -> '1d'
        adjusted_resolution = max(resolution, self.global_min_resolution)
        # NOTE: returned dfs from retrieve-dataflows should be of adjusted_resolution
        dfs_from_storage_per_date: dict[datetime.date, tDataFrame | None] = self.retrieve(
            product,
            adjusted_resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            data_layer=data_layer,
            data_domain=data_domain,
            from_storage=from_storage,
            storage_configs=storage_configs,
            concat_output=False,
            **product_specs
        )
        missing_dates = [date for date in dfs_from_storage_per_date if dfs_from_storage_per_date[date] is None]
        dfs_from_storage = [df for df in dfs_from_storage_per_date.values() if df is not None]


        dfs_from_source: list[tDataFrame] = []
        if missing_dates:
            # REVIEW: check if the condition here is correct, can't afford casually downloading paid data and incur charges
            if self.data_source.access_type != DataAccessType.PAID_BY_USAGE:
                dfs_from_source_per_date = self.download(
                    product,
                    symbol=symbol,
                    resolution=adjusted_resolution,
                    rollback_period='',
                    start_date=str(missing_dates[0]),
                    end_date=str(missing_dates[-1]),
                    data_origin=data_origin,
                    data_layer=data_layer,
                    data_domain=data_domain,
                    to_storage='cache',
                    storage_configs=storage_configs,
                    concat_output=False,
                    **product_specs
                )

                missing_dates = [date for date in dfs_from_source_per_date if dfs_from_source_per_date[date] is None]
                dfs_from_source = [df for df in dfs_from_source_per_date.values() if df is not None]

        if missing_dates:
            self.logger.warning(
                f'output data is INCOMPLETE, '
                f'there are missing data when getting historical {resolution} data for {product},\n'
                f'missing dates: {missing_dates}'
            )
            

        dfs: list[Frame] = [nw.from_native(df) for df in dfs_from_storage + dfs_from_source]
        df: Frame | None = nw.concat(df for df in dfs if not is_empty_dataframe(df)) if dfs else None
        if df is not None:
            df: Frame = df.sort(by='date', descending=False)
            is_resample_required = resolution < adjusted_resolution
            if is_resample_required:
                df: pd.DataFrame = etl.resample_data(df, resolution)
                self.logger.debug(f'resampled {product.name} {adjusted_resolution} data to {resolution}')
            else:
                df: tDataFrame = df.to_native()
            df: tDataFrame = convert_to_user_df(df, self.data_tool.name)
        return df
    
    # TODO
    @clear_subflows
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
    def _create_stream_dataflow(
        self,
        bytewax_source: BytewaxSource | BytewaxStream | str | None=None,
    ) -> DataFlow:
        data_model = self.create_data_model(...)
        dataflow: DataFlow = self._extract_stream(data_model, bytewax_source=bytewax_source)
        return dataflow
        
    # TODO
    def _add_default_transformations_to_stream(self):
        pass
    
    # TODO: General-purpose data fetching without storage overhead
    @clear_subflows
    def fetch(self) -> tDataFrame | None | MarketFeed:
        raise NotImplementedError(f"{self.name} fetch() is not implemented")
    
    # TODO
    def get_realtime_data(self) -> tDataFrame | None:
        assert not self._pipeline_mode, 'pipeline mode is not supported in get_realtime_data()'
        self.fetch()
