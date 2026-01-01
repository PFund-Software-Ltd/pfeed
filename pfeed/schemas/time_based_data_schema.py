import datetime

import pandera.pandas as pa
from pandera.typing import Series


class TimeBasedDataSchema(pa.DataFrameModel):
    """Base schema for all time-based data with monotonically increasing date validation."""

    date: Series[datetime.datetime]

    @pa.check('date', error='date is not monotonic increasing')
    def validate_date(cls, date: Series[datetime.datetime]) -> bool:
        return date.is_monotonic_increasing
