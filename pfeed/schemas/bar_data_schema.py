import datetime

import pandera as pa
from pandera.typing import Series
import pandas as pd

from pfeed.schemas import MarketDataSchema


class BarDataSchema(MarketDataSchema):
    open: Series[float] = pa.Field(gt=0)
    high: Series[float] = pa.Field(gt=0)
    low: Series[float] = pa.Field(gt=0)
    close: Series[float] = pa.Field(gt=0)
    volume: Series[float] = pa.Field(gt=0)

    @pa.dataframe_check
    def validate_high_is_highest(cls, df: pd.DataFrame) -> Series[bool]:
        return (df['high'] >= df['open']) & (df['high'] >= df['low']) & (df['high'] >= df['close'])
    
    @pa.dataframe_check
    def validate_low_is_lowest(cls, df: pd.DataFrame) -> Series[bool]:
        return (df['low'] <= df['open']) & (df['low'] <= df['high']) & (df['low'] <= df['close'])

    @pa.dataframe_check
    def validate_open_within_high_low(cls, df: pd.DataFrame) -> Series[bool]:
        return (df['open'] >= df['low']) & (df['open'] <= df['high'])

    @pa.dataframe_check
    def validate_close_within_high_low(cls, df: pd.DataFrame) -> Series[bool]:
        return (df['close'] >= df['low']) & (df['close'] <= df['high'])

    @pa.check('ts')
    def validate_no_duplicate_timestamps(cls, ts: Series[datetime.datetime]) -> bool:
        return ts.duplicated().sum() == 0
