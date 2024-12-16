from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfeed.types.core import tDataFrame
    from pfeed.types.literals import tSTORAGE
    from pfeed.flows.dataflow import DataFlow
    

from collections import defaultdict

import pandas as pd
from rich.console import Console

from pfeed import etl
from pfeed.utils.utils import get_dates_in_between
from pfund.datas.resolution import Resolution
from pfeed.feeds.base_feed import BaseFeed
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.storages.base_storage import BaseStorage
from pfeed.const.enums import DataRawLevel
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
    
    def create_market_data_model(
        self,
        product: str,
        resolution: str | Resolution,
        date: datetime.date,
        raw_level: DataRawLevel,
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
            resolution=resolution if isinstance(resolution, Resolution) else Resolution(resolution),
            date=date,
            compression=compression,
            metadata=self._create_metadata(raw_level),
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
        )
    
    def _create_metadata(self, raw_level: DataRawLevel) -> dict:
        return {'raw_level': raw_level.name.lower()}
    
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
    
    def _add_default_transformations_to_download(self, dataflows_per_pdt: dict[str, list[DataFlow]], resolution: Resolution, raw_level: DataRawLevel):
        if raw_level != DataRawLevel.ORIGINAL:
            self.transform(etl.convert_to_pandas_df, self._normalize_raw_data)
            for pdt in dataflows_per_pdt:
                self.transform(
                    lambda_with_name('etl.organize_columns', lambda df: etl.organize_columns(df, resolution, product=pdt)),
                    dataflows=dataflows_per_pdt[pdt],
                )
            if raw_level == DataRawLevel.CLEANED:
                transformations = [etl.filter_non_standard_columns]
                # only resample if raw_level is 'cleaned', otherwise, can't resample non-standard columns
                if resolution < self.data_source.lowest_resolution:
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
        self.transform(
            lambda_with_name('etl.convert_to_pandas_df', lambda df: etl.convert_to_pandas_df(df))
        )
        super().load(storage, dataflows=dataflows, **kwargs)
    
    
    def _get_historical_data_from_storage(
        self,
        product: str,
        resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        raw_level: DataRawLevel,
        unique_identifier: str = '',
        from_storage: tSTORAGE | None=None,
    ) -> tuple[tDataFrame | None, list[datetime.date]]:
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        missing_dates: list[datetime.date] = []  # dates without data
        storages: defaultdict[tSTORAGE, list[BaseStorage]] = defaultdict(list)

        for date in dates:
            data_model = self.create_market_data_model(product, resolution, date, raw_level, unique_identifier=unique_identifier)
            if storage := etl.extract_data(data_model, storage=from_storage):
                storages[storage.name.value].append(storage)
                self.logger.info(f'loaded from {storage}')
            else:
                missing_dates.append(date)

        if missing_dates:
            # make missing dates consecutive so that there will be no duplicated data when df_from_storage and df_from_source are concatenated
            missing_dates = get_dates_in_between(missing_dates[0], missing_dates[-1])
        
        # concatenate data from different storages
        dfs = [
            self.data_tool.read_parquet(
                [storage.file_path for storage in storages if storage.date not in missing_dates], 
                storage=from_storage
            ) for from_storage, storages in storages.items()
        ]
        df: tDataFrame = self.data_tool.concat(dfs)
        return df, missing_dates
    
    def _get_historical_data_from_source(
        self, 
        product: str,
        resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        raw_level: DataRawLevel,
        unique_identifier: str = '',
    ) -> tDataFrame:
        self.download(
            data_type=str(resolution.timeframe).lower(),
            products=product,
            start_date=start_date,
            end_date=end_date,
            raw_level=raw_level.name,
            to_storage='cache',
        )
        df, _ = self._get_historical_data_from_storage(
            product,
            resolution,
            start_date,
            end_date,
            raw_level,
            unique_identifier=unique_identifier,
            from_storage='cache',
        )
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
        to_datetime: bool=True,
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
            to_datetime: convert `ts` to datetime.
                Default is True.
        """
        from pfeed.utils.utils import rollback_date_range
        
        assert not self._pipeline_mode, 'get_historical_data() is not supported in pipeline context'
        start_date, end_date = self._standardize_dates(start_date, end_date) if start_date else rollback_date_range(rollback_period)
        raw_level = DataRawLevel[raw_level.upper()]
        if not isinstance(resolution, Resolution):
            resolution = Resolution(resolution)

        df_from_storage, missing_dates = self._get_historical_data_from_storage(product, resolution, start_date, end_date, raw_level, unique_identifier=unique_identifier, from_storage=from_storage)
        if missing_dates:
            df_from_source = self._get_historical_data_from_source(product, resolution, missing_dates[0], missing_dates[-1], raw_level, unique_identifier=unique_identifier)
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
        
        resample_required = resolution.is_day() and resolution.period != 1 and raw_level != DataRawLevel.ORIGINAL
        if df is not None and (resample_required or to_datetime):
            df = self.data_tool.to_datetime(df)
        
        # resample daily data to e.g. '3d'
        if resample_required:
            df = etl.convert_to_pandas_df(df)
            df = etl.resample_data(df, resolution)
            df = etl.convert_to_user_df(df, self.data_tool.name)
            self.logger.info(f'resampled {self.name} {product} daily data to {resolution=}')

        return df