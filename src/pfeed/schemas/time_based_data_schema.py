from typing import Annotated, Final

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime

# column names of cleaned time-based data, typed as str for code referring to them
DATE: Final = "date"


class TimeBasedDataSchema(pa.DataFrameModel):
    """Base schema for all time-based data with monotonically increasing date validation."""

    # Cleaned time-based data uses nanosecond precision, tz-naive. Annotating with a
    # bare `datetime.datetime` would default to Datetime("us") (polars' default) and
    # reject ns data; pin "ns" explicitly. IOs that can't store ns (e.g. DeltaLakeIO) raise
    # instead of downcasting — not the schema's concern.
    # Annotated args map to DateTime(time_zone_agnostic, time_zone, time_unit).
    date: Annotated[DateTime, False, None, "ns"] = pa.Field(alias=DATE)

    @pa.check(DATE, error="date is not monotonic increasing")
    @classmethod
    def validate_date(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            pl.col(data.key).cast(pl.Int64).diff().fill_null(0) >= 0
        )
