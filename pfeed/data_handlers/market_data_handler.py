from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd
    from pfeed.data_models.market_data_model import MarketDataModel
    
import datetime
from pathlib import Path

from pfund.datas.resolution import Resolution
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class MarketDataHandler(TimeBasedDataHandler):
    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import MarketDataSchema, TickDataSchema, BarDataSchema
        # reset index to avoid pandera.errors.SchemaError: DataFrameSchema failed series or dataframe validator 0: <Check validate_index_reset>
        df = df.reset_index(drop=True)
        data_model: MarketDataModel = self._data_model
        resolution: Resolution = data_model.resolution
        if resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
        elif resolution.is_tick():
            schema = TickDataSchema
        elif resolution.is_bar():
            schema = BarDataSchema
        else:
            schema = MarketDataSchema
        return schema.validate(df)

    def _create_filename(self, date: datetime.date) -> str:
        product = self._data_model.product
        filename = '_'.join([product.symbol, str(date)])
        file_extension = self._get_file_extension()
        return filename + file_extension

    def _create_storage_path(self, date: datetime.date | None=None) -> Path:
        data_model: MarketDataModel = self._data_model
        product = data_model.product
        path = (
            Path(f'env={data_model.env.value}')
            / f'data_source={data_model.data_source.name}'
            / f'data_origin={data_model.data_origin}'
            / f'asset_type={str(product.asset_type)}'
            / f'symbol={product.symbol}'
            / f'resolution={repr(data_model.resolution)}'
        )
        if self._is_using_table_format():
            return path
        else:
            assert date is not None, 'date is required for non-table format'
            year, month, day = str(date).split('-')
            return (
                path 
                / f'year={year}' 
                / f'month={month}' 
                / f'day={day}'
            )
