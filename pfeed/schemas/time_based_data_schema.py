import datetime

import polars as pl
import pandera.polars as pa


class TimeBasedDataSchema(pa.DataFrameModel):
    """Base schema for all time-based data with monotonically increasing date validation."""

    date: datetime.datetime

    @pa.check('date', error='date is not monotonic increasing')
    def validate_date(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(
            pl.col(data.key).cast(pl.Int64).diff().fill_null(0) >= 0
        )
