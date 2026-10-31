from typing import Final

import pandera.polars as pa
import polars as pl
from pfund_blocks.resolution import Resolution

from pfeed.schemas.market_data_schema import RESOLUTION, VOLUME, MarketDataSchema
from pfeed.schemas.time_based_data_schema import DATE

# column names of cleaned bar data, typed as str for code referring to them
OPEN: Final = "open"
HIGH: Final = "high"
LOW: Final = "low"
CLOSE: Final = "close"


def _bar_seconds(value: str) -> int | None:
    """Bar length in seconds, or None if it has no fixed length (month, year) or value is invalid."""
    try:
        resolution = Resolution(value)
    except ValueError:
        return None
    if not resolution.is_bar() or resolution.is_month() or resolution.is_year():
        return None
    return resolution.to_seconds()


def _is_bar_resolution(value: str) -> bool:
    try:
        return Resolution(value).is_bar()
    except ValueError:
        return False


class BarDataSchema(MarketDataSchema):
    # lt=inf rejects inf; NaN is already rejected since pandera treats it as null
    open: float = pa.Field(alias=OPEN, lt=float("inf"))
    high: float = pa.Field(alias=HIGH, lt=float("inf"))
    low: float = pa.Field(alias=LOW, lt=float("inf"))
    close: float = pa.Field(alias=CLOSE, lt=float("inf"))
    # gt=0: a period without trades has no bar, a zero-volume bar has made-up prices
    volume: float = pa.Field(alias=VOLUME, gt=0, lt=float("inf"))

    @pa.check(RESOLUTION, error="resolution is not a bar resolution")
    @classmethod
    def validate_bar_resolution(cls, data: pa.PolarsData) -> pl.LazyFrame:
        # e.g. bars resampled from ticks still labeled '1_TICK', which the date gap check would skip
        return data.lazyframe.select(
            pl.col(data.key)
            .unique()
            .map_elements(_is_bar_resolution, return_dtype=pl.Boolean)
            .all()
        )

    @pa.check(DATE)
    @classmethod
    def validate_no_duplicate_timestamps(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            pl.col(data.key).n_unique() == pl.col(data.key).len()
        )

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
            # low <= high is already checked in validate_high_is_highest
            (pl.col(LOW) <= pl.col(OPEN)) & (pl.col(LOW) <= pl.col(CLOSE))
        )

    @pa.dataframe_check(error="date gaps are not whole multiples of the resolution")
    @classmethod
    def validate_date_gaps_are_multiples_of_resolution(
        cls, data: pa.PolarsData
    ) -> pl.LazyFrame:
        # Bars without trades are skipped, so gaps can be any whole multiple of the resolution,
        # e.g. 1m bars can be 1m, 3m or 10m apart, but never 90s; offset bars don't change the gaps.
        # It catches 1m bars labeled 2_MINUTE, but not 2m bars labeled 1_MINUTE, which look exactly like
        # 1m bars of a product trading every other minute, so the label is tested where pfeed writes it.
        # Month and year bars have no fixed length and are skipped, the same for invalid resolutions,
        # which validate_resolution rejects.
        bar_seconds = (
            pl.col(RESOLUTION).first().map_elements(_bar_seconds, return_dtype=pl.Int64)
        )
        gaps = pl.col(DATE).diff().dt.total_nanoseconds()
        return data.lazyframe.select(
            (gaps % (bar_seconds * 1_000_000_000) == 0).fill_null(True)
        )
