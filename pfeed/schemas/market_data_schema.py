import datetime

import pandas as pd
import pandera as pa
from pandera.typing import Series


class MarketDataSchema(pa.DataFrameModel):
    date: Series[datetime.datetime]
    resolution: Series[str] = pa.Field(isin=[
        '1q_L1', '1q_L2', '1q_L3', '1t',
        '1s', '1m', '1h', '1d',
        '1w', '1M', '1y',
    ])
    product: Series[str]
    symbol: Series[str] | None
    
    @pa.check('date', error='date is not monotonic increasing')
    def validate_date(cls, date: Series[datetime.datetime]) -> bool:
        return date.is_monotonic_increasing

    @pa.dataframe_check
    def validate_index_reset(cls, df: pd.DataFrame) -> bool:
        return (
            isinstance(df.index, pd.RangeIndex) and 
            df.index.start == 0 and 
            df.index.step == 1
        )

    @pa.check('resolution')
    def validate_unique_resolution(cls, resolution: Series[str]) -> bool:
        return resolution.nunique() == 1

    @pa.check('product')
    def validate_unique_product(cls, product: Series[str]) -> bool:
        return product.nunique() == 1

    @pa.check('symbol')
    def validate_unique_symbol(cls, symbol: Series[str]) -> bool:
        return symbol.nunique() == 1
