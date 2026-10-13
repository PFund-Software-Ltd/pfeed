from typing import Annotated, Final

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime

# column names of cleaned time-based data, typed as str for code referring to them
DATE: Final = "date"


class TimeBasedDataSchema(pa.DataFrameModel):
    """Base schema for all time-based data with monotonically increasing date validation."""

    # date must be Datetime("ns"), tz-naive with values in UTC, i.e. DateTime(time_zone_agnostic=False, time_zone=None, time_unit="ns").
    # The unit is explicit because a bare `datetime.datetime` annotation means Datetime("us").
    # IOs that store only microseconds (e.g. DeltaLakeIO) cast it themselves.
    date: Annotated[DateTime, False, None, "ns"] = pa.Field(alias=DATE)

    @pa.check(DATE, error="date is not monotonic increasing")
    @classmethod
    def validate_date(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            pl.col(data.key).cast(pl.Int64).diff().fill_null(0) >= 0
        )
