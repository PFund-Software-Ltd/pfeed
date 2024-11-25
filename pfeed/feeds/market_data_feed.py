from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfeed.types.core import tDataFrame
    from pfeed.types.literals import tSTORAGE

from collections import defaultdict

import pandas as pd

from pfeed import etl
from pfeed.utils.utils import get_dates_in_between
from pfund.datas.resolution import Resolution
from pfeed.feeds.base_feed import BaseFeed
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.storages.base_storage import BaseStorage
from pfeed.const.enums import DataRawLevel


class MarketDataFeed(BaseFeed):
    def _print_download_msg(self, resolution: Resolution, start_date: datetime.date, end_date: datetime.date):
        from rich.console import Console
        Console().print(f'Downloading historical {resolution} data from {self.name}, from {str(start_date)} to {str(end_date)} (UTC)', style='bold yellow')
    
    def create_market_data_model(
        self,
        product: str,
        resolution: str | Resolution,
        date: datetime.date,
        unique_identifier: str = '',
        compression: str = 'zstd',
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
        )
    
    def _create_metadata(self, raw_level: Literal['cleaned', 'normalized', 'original']) -> dict:
        return {'raw_level': raw_level.lower()}
    
    def _get_historical_data_from_storage(
        self,
        product: str,
        resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        raw_level: Literal['cleaned', 'normalized', 'original'],
        unique_identifier: str = '',
        from_storage: tSTORAGE | None=None,
    ) -> tuple[tDataFrame | None, list[datetime.date]]:
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        missing_dates: list[datetime.date] = []  # dates without data
        storages: defaultdict[tSTORAGE, list[BaseStorage]] = defaultdict(list)
        metadata = self._create_metadata(raw_level)

        for date in dates:
            data_model = self.create_market_data_model(product, resolution, date, unique_identifier=unique_identifier)
            if storage := etl.extract_data(data_model, storage=from_storage, metadata=metadata):
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
        raw_level: Literal['cleaned', 'normalized', 'original'],
        unique_identifier: str = '',
    ) -> tDataFrame:
        self.download(
            data_type=str(resolution.timeframe).lower(),
            products=product,
            start_date=start_date,
            end_date=end_date,
            raw_level=raw_level,
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
        resolution: str="1d",
        rollback_period: str="1w",
        start_date: str="",
        end_date: str="",
        raw_level: Literal['cleaned', 'normalized', 'original']='normalized',
        from_storage: tSTORAGE | None=None,
        unique_identifier: str='',
    ) -> tDataFrame | None:
        """Get historical data from the data source, local storage or cache.
        Args:
            product: Product symbol, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
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
        """
        from pfeed.utils.utils import rollback_date_range
        
        assert not self._pipeline_mode, 'get_historical_data() is not supported in pipeline context'
        start_date, end_date = self._standardize_dates(start_date, end_date) if start_date else rollback_date_range(rollback_period)
        raw_level = DataRawLevel[raw_level.upper()]
        raw_level_str = raw_level.value.lower()
        if not isinstance(resolution, Resolution):
            resolution = Resolution(resolution)

        df_from_storage, missing_dates = self._get_historical_data_from_storage(product, resolution, start_date, end_date, raw_level_str, unique_identifier=unique_identifier, from_storage=from_storage)
        if missing_dates:
            df_from_source = self._get_historical_data_from_source(product, resolution, missing_dates[0], missing_dates[-1], raw_level_str, unique_identifier=unique_identifier)
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
        if df is not None and resolution.is_day() and resolution.period != 1 and raw_level != DataRawLevel.ORIGINAL:
            if not isinstance(df, pd.DataFrame):
                df = etl.convert_to_pandas_df(df)
            df = etl.resample_data(df, resolution)
            df = etl.convert_to_user_df(df, self.data_tool.name)
            self.logger.info(f'resampled {self.name} {product} daily data to {resolution=}')

        return df