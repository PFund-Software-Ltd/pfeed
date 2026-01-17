from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pfeed.data_models.news_data_model import NewsDataModel

import datetime
from pathlib import Path

import pandas as pd
import polars as pl

from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class NewsDataHandler(TimeBasedDataHandler):
    DEFAULT_FILENAME = 'GENERAL_MARKET_NEWS'

    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import NewsDataSchema
        # reset index to avoid pandera.errors.SchemaError: DataFrameSchema failed series or dataframe validator 0: <Check validate_index_reset>
        df = df.reset_index(drop=True)
        schema = NewsDataSchema
        return schema.validate(df)
    
    def _create_filename(self, date: datetime.date) -> str:
        product = self._data_model.product
        name = self.DEFAULT_FILENAME if product is None else product.symbol
        filename = '_'.join([name, str(date)])
        file_extension = self._get_file_extension()
        return filename + file_extension    

    def _create_storage_path(self, date: datetime.date | None=None) -> Path:
        data_model: NewsDataModel = self._data_model
        product = data_model.product
        asset_type = 'NONE' if product is None else str(product.asset_type)
        symbol = 'NONE' if product is None else product.symbol
        path = (
            Path()
            / f'asset_type={asset_type}'
            / f'symbol={symbol}'   
        )
        if self._is_table_io():
            return path
        elif self._is_database_io():
            return Path()
        else:
            assert date is not None, 'date is required for non-table format'
            year, month, day = str(date).split('-')
            return (
                path 
                / f'year={year}' 
                / f'month={month}' 
                / f'day={day}'
            )

    def read(self, **io_options) -> tuple[pl.LazyFrame | None, dict[str, Any]]:
        df, metadata = super().read(**io_options)
        if df is not None:
            # NOTE: fill null with empty string, otherwise concat will fail when column A in df1 is of type String and column A in df2 is of type null
            nullable_columns = ['product', 'author', 'exchange', 'symbol']
            for col in nullable_columns:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.String))
        return df, metadata