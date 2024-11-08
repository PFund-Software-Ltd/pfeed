"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import datetime
    from pfeed.types.core import tDataModel
    from pfeed.types.literals import tSTORAGE, tDATA_TOOL

import pandas as pd
from rich.console import Console

from pfeed.feeds.base_feed import clear_current_dataflows
from pfeed.feeds.crypto_feed import CryptoFeed
from pfeed import etl
from pfeed.utils.utils import lambda_with_name

__all__ = ['BybitFeed']

tPRODUCT_TYPE = Literal['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT', 'OPT']


class BybitFeed(CryptoFeed):
    MAPPING_COLS = {'Buy': 1, 'Sell': -1}
    RENAMING_COLS = {'timestamp': 'ts', 'size': 'volume'}
    
    def __init__(
        self, 
        data_tool: tDATA_TOOL='pandas', 
        use_ray: bool=True,
        use_prefect: bool=False,
        pipeline_mode: bool=False,
    ):
        from pfeed.sources.bybit.data_source import BybitDataSource
        super().__init__(
            data_source=BybitDataSource(),
            data_tool=data_tool, 
            use_ray=use_ray,
            use_prefect=use_prefect,
            pipeline_mode=pipeline_mode, 
        )
    
    def _normalize_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Normalizes raw data by renaming columns, mapping columns, and converting timestamp.
        bytes (any format, e.g. csv.gzip) in, bytes (parquet file) out.
        '''
        df = df.rename(columns=self.RENAMING_COLS)
        df['side'] = df['side'].map(self.MAPPING_COLS)
            
        # standardize `ts` column
        # NOTE: for ptype SPOT, unit is 'ms', e.g. 1671580800123, in milliseconds
        unit = 'ms' if df['ts'][0] > 10**12 else 's'  # REVIEW
        # NOTE: somehow some data is in reverse order, e.g. BTC_USDT_PERP in 2020-03-25
        is_in_reverse_order = df['ts'][0] > df['ts'][1]
        if is_in_reverse_order:
            df['ts'] = df['ts'][::-1].values
        # NOTE: this may make the `ts` value inaccurate, e.g. 1671580800.9906 -> 1671580800.990600192
        df['ts'] = pd.to_datetime(df['ts'], unit=unit)
        return df
    
    # TODO
    @clear_current_dataflows
    def stream(self) -> BybitFeed:
        raise NotImplementedError(f'{self.name} stream() is not implemented')
        return self
    
    # TODO
    def _execute_stream(self, data_model: tDataModel):
        raise NotImplementedError(f'{self.name} _execute_stream() is not implemented')

    @clear_current_dataflows
    def download(
        self,
        products: str | list[str] | None=None, 
        product_types: tPRODUCT_TYPE | list[tPRODUCT_TYPE] | None=None, 
        data_type: Literal['tick', 'second', 'minute', 'hour', 'day', 'week', 'month', 'year']='tick',
        start_date: str='',
        end_date: str='',
        raw_level: Literal['normalized', 'cleaned', 'original']='normalized',
        storage: tSTORAGE='local',
    ) -> BybitFeed:
        from pfund.datas.resolution import Resolution
        from pfeed.const.enums import DataRawLevel
        from pfeed.utils.utils import get_dates_in_between

        pdts = self._prepare_pdts(products, product_types)
        resolution = Resolution(data_type)
        start_date, end_date = self._standardize_dates(start_date, end_date)
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        raw_level = DataRawLevel[raw_level.upper()]
        is_raw_data = resolution >= self.data_source.lowest_resolution
        if not is_raw_data:
            raw_level = DataRawLevel.CLEANED
         
        Console().print(f'Downloading historical {resolution} data from {self.name}, {start_date=} {end_date=}', style='bold yellow')
        for pdt in pdts:
            for date in dates:
                data_model = self.create_market_data_model(pdt, resolution, date)
                # create a dataflow that will schedule _execute_download()
                super().extract('download', data_model)
        if raw_level != DataRawLevel.ORIGINAL:
            transformations = [
                self._normalize_raw_data,
                lambda_with_name('etl.organize_columns', lambda df: etl.organize_columns(df, pdt, resolution)),
            ]
            if raw_level == DataRawLevel.CLEANED:
                transformations.append(etl.filter_non_standard_columns)
                if not resolution.is_tick():
                    transformations.append(
                        lambda_with_name('etl.resample_data', lambda df: etl.resample_data(df, resolution))
                    )
            self.transform(*transformations)
        if not self._pipeline_mode:
            self.load(storage)
            self.run()
        else:
            self.transform(
                lambda_with_name('etl.convert_to_user_df', lambda df: etl.convert_to_user_df(df, self.data_tool.name))
            )
        return self

    def _execute_download(self, data_model: tDataModel) -> pd.DataFrame | None:
        raw_data: bytes | None = self.data_source.download_market_data(data_model.product, data_model.date)
        if raw_data is None:
            return None
        else:
            df: pd.DataFrame = etl.convert_to_pandas_df(raw_data)
            return df
