from __future__ import annotations
from typing import TYPE_CHECKING, Any

import pandas as pd
import polars as pl

from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler


class NewsDataHandler(TimeBasedDataHandler):
    def _validate_schema(self, data: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import NewsDataSchema
        schema = NewsDataSchema
        return schema.validate(data)

    def read(self, delta_version: int | None=None) -> tuple[pl.LazyFrame | None, dict[str, Any]]:
        df, metadata = super().read(delta_version=delta_version)
        if df is not None:
            # NOTE: fill null with empty string, otherwise concat will fail when column A in df1 is of type String and column A in df2 is of type null
            nullable_columns = ['product', 'author', 'exchange', 'symbol']
            for col in nullable_columns:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.String))
        return df, metadata