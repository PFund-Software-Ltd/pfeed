import warnings
import datetime

import pandas as pd
import pandera as pa
from pandera.typing import Series
    
from pfeed.schemas import MarketDataSchema


class BarDataSchema(MarketDataSchema):
    open: Series[float] = pa.Field(gt=0)
    high: Series[float] = pa.Field(gt=0)
    low: Series[float] = pa.Field(gt=0)
    close: Series[float] = pa.Field(gt=0)
    volume: Series[float] = pa.Field(ge=0)

    @pa.dataframe_check
    def warn_zero_volume(cls, df: pd.DataFrame) -> bool:
        zero_volume_df = df[df['volume'] == 0]
        if not zero_volume_df.empty:
            YELLOW = "\033[93m"  # color for warning
            RESET = "\033[0m"  # Reset to default color
            warning_message = (
                f"{YELLOW}⚠️ Warning: The following rows have zero volume:\n"
                # Format the zero volume rows as a string for better readability.
                f"{zero_volume_df.to_string()}{RESET}"
            )
            warnings.warn(warning_message, UserWarning)
        return True  # Always return True to ensure the schema validation doesn't fail

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

    @pa.check('date')
    def validate_no_duplicate_timestamps(cls, ts: Series[datetime.datetime]) -> bool:
        return ts.duplicated().sum() == 0
