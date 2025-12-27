from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import GenericFrame

import datetime

import pandas as pd

from pfeed.data_handlers.market_data_handler import MarketDataHandler
from pfeed.enums import DataTool


class YahooFinanceMarketDataHandler(MarketDataHandler):
    def _write_batch(self, df: GenericFrame):
        from pfeed._etl.base import convert_to_desired_df
        df: pd.DataFrame = convert_to_desired_df(df, DataTool.pandas)
        df.reset_index(inplace=True)
        columns = df.columns
        # raw df = df directly from yfinance, not the normalized one
        is_raw_df = 'Date' in columns or 'Datetime' in columns
        kwargs = {
            'validate': not is_raw_df,
        }
        if is_raw_df:
            kwargs['date_column'] = 'Datetime' if 'Datetime' in columns else 'Date'
        super()._write_batch(df, **kwargs)
        
    # TODO: handle raw data streaming
    # def _standardize_streaming_data(self, data: dict) -> dict:
    #     mts = data['ts']  # in ms
    #     date = datetime.datetime.fromtimestamp(
    #         mts / 1000,
    #         tz=datetime.timezone.utc
    #     ).replace(tzinfo=None)
    #     data['date'] = date

    #     # add year, month, day columns for delta table partitioning
    #     data['year'] = date.year
    #     data['month'] = date.month
    #     data['day'] = date.day 
    #     return data
    