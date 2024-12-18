from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfund.products.product_base import BaseProduct
    from pfeed.types.core import tDataFrame
    from pfeed.types.literals import tSTORAGE, tPRODUCT_TYPE
    from pfeed.flows.dataflow import DataFlow
    from pfeed.storages.base_storage import BaseStorage

from collections import defaultdict

import pandas as pd
from rich.console import Console

from pfund.datas.resolution import Resolution
from pfeed import etl
from pfeed.feeds.base_feed import BaseFeed, clear_current_dataflows
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.const.enums import DataRawLevel, MarketDataType, DataAccessType
from pfeed.utils.utils import lambda_with_name
from pfeed.data_tools.data_tool_pandas import to_datetime as pandas_to_datetime


class MarketDataFeed(BaseFeed):
    def _print_download_msg(self, resolution: Resolution, start_date: datetime.date, end_date: datetime.date, raw_level: DataRawLevel):
        Console().print(f'Downloading historical {resolution} data from {self.name}, from {str(start_date)} to {str(end_date)} (UTC), raw_level={raw_level.name}', style='bold yellow')
    
    def _print_original_raw_level_msg(self):
        Console().print(
            f'Warning: {self.name} data with raw_level="original" will NOT be compatible with pfund backtesting. \n'
            'Use it only for data exploration or if you plan to use other backtesting frameworks.',
            style='bold magenta'
        )
    
    def _is_resolution_supported(self, resolution: Resolution) -> bool:
        '''Checks if the resolution is natively supported by the data source.'''
        is_resolution_supported = is_period_supported = True
        global_lowest_resolution = Resolution('1' + [dt.name for dt in MarketDataType][-1])
        if resolution < min(global_lowest_resolution, self.data_source.lowest_resolution):
            is_resolution_supported = False
        elif hasattr(self, 'SUPPORTED_RESOLUTIONS') and \
            resolution.period not in self.SUPPORTED_RESOLUTIONS[repr(resolution.timeframe)]:
            is_period_supported = False
        return is_resolution_supported and is_period_supported
    
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
            source=self.data_source,
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
        is_resample_required = (not self._is_resolution_supported(resolution))
        if raw_level != DataRawLevel.CLEANED and is_resample_required:
            raw_level = DataRawLevel.CLEANED
            self.logger.debug(f'{self.name} raw_level is set to {raw_level} when resampling is required')
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
        self._add_default_transformations_to_download(dataflows_per_pdt, resolution, raw_level)
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
        return {'raw_level': raw_level.name.lower(), 'is_placeholder': 'false'}
    
    # TODO
    def _assert_standards(self, df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        '''
        Assert that the data conforms to the pfeed's internal standards.
        For market data, the standards are:
        - values in 'ts' column must be of unit 's', e.g. 1704067200.123, but not 1704067200123
        - 'ts' column must be sorted in ascending order
        - 'ts' must be of 'float' type
        - 'ts', 'product', 'resolution' columns must exist
        '''
        raw_level = metadata['raw_level']
        if raw_level == 'original':
            return df
        else:
            # TODO: assert standards for 'cleaned' and 'normalized' raw_level
            return df
    
    def _add_default_transformations_to_download(
        self,
        dataflows_per_pdt: dict[str, list[DataFlow]],
        resolution: Resolution,
        raw_level: DataRawLevel,
    ):
        if raw_level != DataRawLevel.ORIGINAL:
            self.transform(etl.convert_to_pandas_df, self._normalize_raw_data, pandas_to_datetime)
            for product_name in dataflows_per_pdt:
                self.transform(
                    lambda_with_name('etl.organize_columns', lambda df: etl.organize_columns(df, resolution, product_name)),
                    dataflows=dataflows_per_pdt[product_name],
                )
            if raw_level == DataRawLevel.CLEANED:
                transformations = [etl.filter_non_standard_columns]
                # only resample if raw_level is 'cleaned', otherwise, can't resample non-standard columns
                if not self._is_resolution_supported(resolution):
                    transformations.append(
                        lambda_with_name('etl.resample_data', lambda df: etl.resample_data(df, resolution))
                    )
                self.transform(*transformations)
        else:
            self._print_original_raw_level_msg()
        if self._pipeline_mode:
            self.transform(
                lambda_with_name('etl.convert_to_user_df', lambda df: etl.convert_to_user_df(df, self.data_tool.name))
            )
    
    def load(self, storage: tSTORAGE='local', dataflows: list[DataFlow] | None=None, **kwargs):
        # convert back to pandas dataframe before calling etl.write_data() in load()
        self.transform(etl.convert_to_pandas_df)
        super().load(storage, dataflows=dataflows, **kwargs)
    
    def _get_historical_data_from_storage(
        self,
        product: BaseProduct,
        unit_resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        raw_level: DataRawLevel,
        unique_identifier: str = '',
        from_storage: tSTORAGE | None=None,
    ) -> tuple[tDataFrame | None, list[datetime.date]]:
        from pfeed.utils.utils import get_dates_in_between
        assert unit_resolution.period == 1, 'unit_resolution must have period = 1'
        is_data_resampled = (not self._is_resolution_supported(unit_resolution))
        if raw_level != DataRawLevel.CLEANED and is_data_resampled:
            raw_level = DataRawLevel.CLEANED
            self.logger.debug(f'{self.name} raw_level is auto-adjusted to {raw_level} since {product} data in storage has been resampled')
        missing_dates: list[datetime.date] = []  # dates without data
        storages: defaultdict[tSTORAGE, list[BaseStorage]] = defaultdict(list)
        
        for date in get_dates_in_between(start_date, end_date):
            data_model = self.create_market_data_model(product, unit_resolution, raw_level, date, unique_identifier=unique_identifier)
            if storage := etl.extract_data(data_model, storage=from_storage):
                storages[storage.name.value].append(storage)
                self.logger.info(f'loaded from {storage}')
            else:
                missing_dates.append(date)

        if missing_dates:
            # since data on missing dates will be downloaded from source using start_date and end_date,
            # downloaded data will be consecutive, so need to make missing dates consecutive
            missing_dates = get_dates_in_between(missing_dates[0], missing_dates[-1])
        
        # concatenate data from different storages
        dfs = [
            self.data_tool.read_parquet(
                # exclude storage.date in missing_dates to avoid duplicated data when df_from_storage and df_from_source are concatenated
                [storage.file_path for storage in storages if storage.date not in missing_dates], 
                storage=from_storage
            ) for from_storage, storages in storages.items()
        ]
        df: tDataFrame | None = self.data_tool.concat(dfs) if dfs else None
        return df, missing_dates
    
    def _get_historical_data_from_source(
        self, 
        product: BaseProduct,
        unit_resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        raw_level: DataRawLevel,
        unique_identifier: str = '',
    ) -> tDataFrame:
        if self.data_source.access_type == DataAccessType.PAID_BY_USAGE:
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
        assert not missing_dates, f'{self.name} {product.name} {unit_resolution} data is missing for dates: {missing_dates}'
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
        
        # e.g. target_resolution = '1w', unit_resolution will be '1w' at first, but since 'week' is not supported, it will be adjusted to '1d'
        target_resolution = Resolution(resolution)
        unit_resolution = Resolution('1' + repr(target_resolution.timeframe))
        while not self._is_resolution_supported(unit_resolution):
            unit_resolution = unit_resolution.higher()
            
        raw_level = DataRawLevel[raw_level.upper()]
        is_resample_required = (target_resolution < unit_resolution)
        if raw_level != DataRawLevel.CLEANED and is_resample_required:
            raw_level = DataRawLevel.CLEANED
            self.logger.debug(f'{self.name} raw_level is set to {raw_level} when resampling is required')

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
            df_from_source = self._get_historical_data_from_source(
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
            self.logger.warning('concatenating and sorting data from storage and source is slow, consider download() data before get_historical_data()')
            df = self.data_tool.concat([df_from_storage, df_from_source])
            df = self.data_tool.sort_by_ts(df)
        elif df_from_storage is not None:
            df = df_from_storage
        elif df_from_source is not None:
            df = df_from_source
        else:
            df = None
        
        # resample daily data to e.g. '3d'
        if df is not None and is_resample_required:                
            df = self.data_tool.resample_data(df, target_resolution)
            self.logger.info(f'resampled {self.name} {product} {unit_resolution} data to {target_resolution=}')

        return df