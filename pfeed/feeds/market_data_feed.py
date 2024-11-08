from __future__ import annotations
from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    import datetime
    from pfeed.types.core import tDataFrame
    from pfeed.types.literals import tSTORAGE

from pfund.datas.resolution import Resolution
from pfeed.feeds.base_feed import BaseFeed
from pfeed.storages.base_storage import BaseStorage
from pfeed.data_models.market_data_model import MarketDataModel


class MarketDataFeed(BaseFeed):
    def create_market_data_model(
        self,
        product: str,
        resolution: str | Resolution,
        date: datetime.date,
        unique_identifier: str = '',
    ) -> MarketDataModel:
        from pfeed.const.enums import Environment
        return MarketDataModel(
            env=Environment.BACKTEST,
            source=self.data_source,
            unique_identifier=unique_identifier,
            product=product,
            resolution=resolution if isinstance(resolution, Resolution) else Resolution(resolution),
            date=date,
        )
    
    # NOTE: this is conceptually the reader for storage
    # since different data have different formats, the read method is handled here
    def _get_historical_data_from_storage(
        self,
        product: str,
        resolution: Resolution,
        start_date: datetime.date,
        end_date: datetime.date,
        unique_identifier: str = '',
        storage: tSTORAGE | None=None,
    ) -> tDataFrame | None:
        from pfeed import etl
        from pfeed.utils.utils import get_dates_in_between

        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        stoarge_literal = storage
        storages: list[BaseStorage] = []
        for date in dates:
            data_model = self.create_market_data_model(product, resolution, date, unique_identifier=unique_identifier)
            if storage := etl.extract_data(data_model, storage=stoarge_literal):
                storages.append(storage)
                
        file_paths = [storage.file_path for storage in storages]
        if file_paths:
            df = self.data_tool.read_parquet(file_paths)
            return df
        else:
            return None
    
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
            storage='cache',
        )
        df = self._get_historical_data_from_storage(
            product,
            resolution,
            start_date,
            end_date,
            unique_identifier=unique_identifier,
            storage='cache',
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
        storage: tSTORAGE | None=None,
        unique_identifier: str='',
    ) -> tDataFrame:
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
            storage: Destinated storage.
                If not specified, will search through all storages, e.g. local, minio, cache.
        """
        from pfeed.utils.utils import rollback_date_range
        
        assert not self._pipeline_mode, 'get_historical_data() is not supported in pipeline context'
        start_date, end_date = self._standardize_dates(start_date, end_date) if start_date else rollback_date_range(rollback_period)
        if not isinstance(resolution, Resolution):
            resolution = Resolution(resolution)

        if (df := self._get_historical_data_from_storage(product, resolution, start_date, end_date, unique_identifier=unique_identifier, storage=storage)) is not None:
            pass
        # TODO: if data with higher resolution is available, use it and resample it to the requested resolution
        # NOTE: it requires each data_tool to implement the resampling logic
        # elif (df := self._get_historical_data_from_storage(product, higher_resolution, start_date, end_date, unique_identifier=unique_identifier, storage=storage)) is not None:
        #     # load to cache as well
        #     pass
        elif (df := self._get_historical_data_from_source(product, resolution, start_date, end_date, raw_level, unique_identifier=unique_identifier)) is not None:
            pass
        else:
            self.logger.warning(f'No historical data found for {product} {resolution} from {self.name}')
        
        # resample daily data to e.g. '3d'
        if df is not None and resolution.is_day() and resolution.period != 1:
            import pandas as pd
            from pfeed import etl
            if not isinstance(df, pd.DataFrame):
                df = etl.convert_to_pandas_df(df)
            df = etl.resample_data(df, resolution)
            self.logger.info(f'resampled {self.name} {product} data to {resolution=}')
            df = etl.convert_to_user_df(df, self.data_tool.name)

        return df