from __future__ import annotations

from typing import ClassVar, Self

import datetime

from pydantic import Field, model_validator

from pfeed.base.data_model import BaseDataModel


class TimeBasedDataModel(BaseDataModel):
    # the date column the feed standardizes the data to, and the data handler partitions by
    DATE_COL_IN_CLEANED_DATA: ClassVar[str] = "date"
    DATE_COL_IN_RAW_DATA: ClassVar[str] = "_pfeed_date"

    start_date: datetime.date = Field(description="Start of the date range.")
    end_date: datetime.date = Field(
        description="End of the date range. Must be greater than or equal to start date."
    )

    @property
    def date(self) -> datetime.date:
        if self.is_date_range():
            raise ValueError(
                "start_date and end_date must be the same for a single date"
            )
        return self.start_date

    @model_validator(mode="after")
    def _validate_date_range(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError(
                f"start date {self.start_date} must be before or equal to end date {self.end_date}."
            )
        return self

    @property
    def dates(self) -> list[datetime.date]:
        import polars as pl

        return pl.date_range(
            self.start_date, self.end_date, interval="1d", eager=True
        ).to_list()

    def is_date_range(self) -> bool:
        return self.start_date != self.end_date

    def __str__(self) -> str:
        if self.is_date_range():
            return ":".join(
                [
                    super().__str__(),
                    "(from)" + str(self.start_date),
                    "(to)" + str(self.end_date),
                ]
            )
        else:
            return ":".join([super().__str__(), str(self.start_date)])
