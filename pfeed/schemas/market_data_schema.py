import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series

from pfeed.schemas.time_based_data_schema import TimeBasedDataSchema
from pfeed.enums import MarketDataType
from pfund.datas.resolution import Resolution


class MarketDataSchema(TimeBasedDataSchema):
    resolution: Series[str] = pa.Field(isin=
        set(repr(Resolution('1'+dtype)) for dtype in MarketDataType.__members__.keys())
    )
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
