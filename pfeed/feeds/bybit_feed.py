"""High-level API for getting historical/streaming data from Bybit."""
from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    import datetime
    from pfeed.types.core import tDataModel
    from pfeed.types.literals import tSTORAGE
    from pfeed.flows.dataflow import DataFlow

import pandas as pd

from pfeed.feeds.base_feed import clear_current_dataflows
from pfeed.feeds.crypto_market_data_feed import CryptoMarketDataFeed


__all__ = ['BybitFeed']
tPRODUCT_TYPE = Literal['SPOT', 'PERP', 'IPERP', 'FUT', 'IFUT', 'OPT']


class BybitFeed(CryptoMarketDataFeed):
    @staticmethod
    def get_data_source():
        from pfeed.sources.bybit.data_source import BybitDataSource
        return BybitDataSource()
    
    def _normalize_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize raw data from Bybit API into standardized format.

        This method performs the following normalizations:
        - Renames columns to standard names (timestamp -> ts, size -> volume)
        - Maps trade side values from Buy/Sell to 1/-1
        - Corrects reverse-ordered data if detected

        Args:
            df (pd.DataFrame): Raw DataFrame from Bybit API containing trade data

        Returns:
            pd.DataFrame: Normalized DataFrame with standardized column names and values
        """
        MAPPING_COLS = {'Buy': 1, 'Sell': -1}
        RENAMING_COLS = {'timestamp': 'ts', 'size': 'volume'}
        df = df.rename(columns=RENAMING_COLS)
        df['side'] = df['side'].map(MAPPING_COLS)
        is_in_reverse_order = df['ts'][0] > df['ts'][1]
        if is_in_reverse_order:
            df = df.iloc[::-1].reset_index(drop=True)
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
        to_storage: tSTORAGE='local',
        filename_prefix: str='',
        filename_suffix: str='',
    ) -> BybitFeed:
        '''
        Args:
            filename_prefix: The prefix of the filename.
            filename_suffix: The suffix of the filename.
        '''
        from pfund.datas.resolution import Resolution
        from pfeed.const.enums import DataRawLevel
        from pfeed.utils.utils import get_dates_in_between

        pdts = self._prepare_products(products, product_types)
        resolution = Resolution(data_type)
        start_date, end_date = self._standardize_dates(start_date, end_date)
        dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
        is_raw_data = resolution >= self.data_source.lowest_resolution
        if not is_raw_data:
            raw_level = DataRawLevel.CLEANED
        else:
            raw_level = DataRawLevel[raw_level.upper()]
        if self.config.print_msg:
            self._print_download_msg(resolution, start_date, end_date, raw_level)
        dataflows_per_pdt: dict[str, list[DataFlow]] = {}
        for pdt in pdts:
            dataflows_per_pdt[pdt] = []
            for date in dates:
                data_model = self.create_market_data_model(pdt, resolution, date, raw_level, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
                # create a dataflow that schedules _execute_download()
                dataflow: DataFlow = super().extract('download', data_model)
                dataflows_per_pdt[pdt].append(dataflow)
        self._add_default_transformations_to_download(dataflows_per_pdt, resolution, raw_level)
        if not self._pipeline_mode:
            self.load(to_storage)
            self.run()
        return self

    def _execute_download(self, data_model: tDataModel) -> bytes | None:
        return self.api.get_data(data_model.product, data_model.date)
