from typing import Final

import warnings

import pandera.polars as pa
import polars as pl

from pfeed.schemas.market_data_schema import VOLUME, MarketDataSchema
from pfeed.schemas.time_based_data_schema import DATE

# column names of cleaned bar data, typed as str for code referring to them
OPEN: Final = "open"
HIGH: Final = "high"
LOW: Final = "low"
CLOSE: Final = "close"


class BarDataSchema(MarketDataSchema):
    open: float = pa.Field(alias=OPEN, gt=0)
    high: float = pa.Field(alias=HIGH, gt=0)
    low: float = pa.Field(alias=LOW, gt=0)
    close: float = pa.Field(alias=CLOSE, gt=0)
    volume: float = pa.Field(alias=VOLUME, ge=0)

    @pa.dataframe_check
    @classmethod
    def warn_zero_volume(cls, data: pa.PolarsData) -> bool:
        zero_volume_df = data.lazyframe.filter(pl.col(VOLUME) == 0).collect()
        if zero_volume_df.height > 0:
            YELLOW = "\033[93m"  # color for warning
            RESET = "\033[0m"  # Reset to default color
            warning_message = (
                f"{YELLOW}⚠️ Warning: The following rows have zero volume:\n"
                f"{zero_volume_df}{RESET}"
            )
            warnings.warn(warning_message, UserWarning, stacklevel=1)
        return True  # Always return True to ensure the schema validation doesn't fail

    @pa.dataframe_check
    @classmethod
    def validate_high_is_highest(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col(HIGH) >= pl.col(OPEN))
            & (pl.col(HIGH) >= pl.col(LOW))
            & (pl.col(HIGH) >= pl.col(CLOSE))
        )

    @pa.dataframe_check
    @classmethod
    def validate_low_is_lowest(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col(LOW) <= pl.col(OPEN))
            & (pl.col(LOW) <= pl.col(HIGH))
            & (pl.col(LOW) <= pl.col(CLOSE))
        )

    @pa.dataframe_check
    @classmethod
    def validate_open_within_high_low(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col(OPEN) >= pl.col(LOW)) & (pl.col(OPEN) <= pl.col(HIGH))
        )

    @pa.dataframe_check
    @classmethod
    def validate_close_within_high_low(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col(CLOSE) >= pl.col(LOW)) & (pl.col(CLOSE) <= pl.col(HIGH))
        )

    @pa.check(DATE)
    @classmethod
    def validate_no_duplicate_timestamps(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            pl.col(data.key).n_unique() == pl.col(data.key).len()
        )
