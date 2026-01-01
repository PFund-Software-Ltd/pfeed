import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

from pfeed.schemas.time_based_data_schema import TimeBasedDataSchema


class MarketDataSchema(TimeBasedDataSchema):
    resolution: Series[str] = pa.Field(isin=[
        '1q_L1', '1q_L2', '1q_L3', '1t',
        '1s', '1m', '1h', '1d',
        '1w', '1M', '1y',
    ])
    symbol: Series[str] | None

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

    @pa.check('symbol')
    def validate_unique_symbol(cls, symbol: Series[str]) -> bool:
        return symbol.nunique() == 1
