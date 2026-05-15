import warnings

import polars as pl
import pandera.polars as pa

from pfeed.schemas import MarketDataSchema


class BarDataSchema(MarketDataSchema):
    open: float = pa.Field(gt=0)
    high: float = pa.Field(gt=0)
    low: float = pa.Field(gt=0)
    close: float = pa.Field(gt=0)
    volume: float = pa.Field(ge=0)

    @pa.dataframe_check
    def warn_zero_volume(cls, data: pa.PolarsData) -> bool:
        zero_volume_df = data.lazyframe.filter(pl.col('volume') == 0).collect()
        if zero_volume_df.height > 0:
            YELLOW = "\033[93m"  # color for warning
            RESET = "\033[0m"  # Reset to default color
            warning_message = (
                f"{YELLOW}⚠️ Warning: The following rows have zero volume:\n"
                f"{zero_volume_df}{RESET}"
            )
            warnings.warn(warning_message, UserWarning)
        return True  # Always return True to ensure the schema validation doesn't fail

    @pa.dataframe_check
    def validate_high_is_highest(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col('high') >= pl.col('open'))
            & (pl.col('high') >= pl.col('low'))
            & (pl.col('high') >= pl.col('close'))
        )

    @pa.dataframe_check
    def validate_low_is_lowest(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col('low') <= pl.col('open'))
            & (pl.col('low') <= pl.col('high'))
            & (pl.col('low') <= pl.col('close'))
        )

    @pa.dataframe_check
    def validate_open_within_high_low(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col('open') >= pl.col('low')) & (pl.col('open') <= pl.col('high'))
        )

    @pa.dataframe_check
    def validate_close_within_high_low(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            (pl.col('close') >= pl.col('low')) & (pl.col('close') <= pl.col('high'))
        )

    @pa.check('date')
    def validate_no_duplicate_timestamps(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            pl.col(data.key).n_unique() == pl.col(data.key).len()
        )
