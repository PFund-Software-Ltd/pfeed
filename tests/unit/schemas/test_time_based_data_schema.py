from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from polars._typing import TimeUnit

import datetime

import pandera.errors
import polars as pl
import pytest

from pfeed.schemas.time_based_data_schema import DATE, TimeBasedDataSchema


def _df(dates: list[datetime.datetime], time_unit: TimeUnit = "ns", time_zone: str | None = None) -> pl.DataFrame:
    return pl.DataFrame({DATE: pl.Series(dates, dtype=pl.Datetime(time_unit, time_zone))})


DATES = [
    datetime.datetime(2025, 1, 1),
    datetime.datetime(2025, 1, 1, 0, 0, 1),
    datetime.datetime(2025, 1, 1, 0, 0, 2),
]


class TestTimeBasedDataSchema:
    def test_valid(self):
        df = _df(DATES)
        assert TimeBasedDataSchema.validate(df).equals(df)

    def test_equal_consecutive_dates_pass(self):
        TimeBasedDataSchema.validate(_df([DATES[0], DATES[0], DATES[1]]))

    def test_decreasing_dates_fail(self):
        with pytest.raises(pandera.errors.SchemaError, match="date is not monotonic increasing"):
            TimeBasedDataSchema.validate(_df([DATES[1], DATES[0], DATES[2]]))

    def test_wrong_time_unit_fails(self):
        with pytest.raises(pandera.errors.SchemaError):
            TimeBasedDataSchema.validate(_df(DATES, time_unit="us"))

    def test_tz_aware_fails(self):
        with pytest.raises(pandera.errors.SchemaError):
            TimeBasedDataSchema.validate(_df(DATES, time_zone="UTC"))

    def test_missing_date_column_fails(self):
        with pytest.raises(pandera.errors.SchemaError):
            TimeBasedDataSchema.validate(pl.DataFrame({"x": [1, 2, 3]}))
