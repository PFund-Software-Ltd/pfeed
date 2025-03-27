from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pfeed.typing import GenericFrame, tDATA_TOOL
    from pfeed.data_models.news_data_model import NewsDataModel

import pandas as pd
import polars as pl

from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class NewsDataHandler(TimeBasedDataHandler):
    def _validate_schema(self, data: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import NewsDataSchema
        schema = NewsDataSchema
        return schema.validate(data)

    def write(self, df: GenericFrame):
        data_model: NewsDataModel = self._data_model
        metadata = {
            'start_date': data_model.start_date, 
            'end_date': data_model.end_date,
            'product': data_model.product.name if data_model.product else None,
            'product_type': data_model.product.type if data_model.product else None,
            'data_source': data_model.data_source.name,
            'data_origin': data_model.data_origin,
        }
        return super().write(df, metadata=metadata)
        
    def read(self, data_tool: tDATA_TOOL='polars', delta_version: int | None=None) -> tuple[GenericFrame | None, dict[str, Any]]:
        df, metadata = super().read(data_tool=data_tool, delta_version=delta_version)
        if df is not None and data_tool == 'polars':
            # NOTE: fill null with empty string, otherwise concat will fail when column A in df1 is of type String and column A in df2 is of type null
            nullable_columns = ['product', 'author', 'exchange', 'symbol']
            for col in nullable_columns:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.String))
        return df, metadata