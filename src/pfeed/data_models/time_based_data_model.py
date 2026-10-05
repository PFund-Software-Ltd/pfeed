from __future__ import annotations

import datetime
from typing import ClassVar

from pydantic import Field, ValidationInfo, field_validator

from pfeed.data_models.base_data_model import BaseDataModel


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
        assert not self.is_date_range(), (
            "start_date and end_date must be the same for a single date"
        )
        return self.start_date

    @field_validator("end_date")
    @classmethod
    def _validate_end_date(
        cls, end_date: datetime.date, info: ValidationInfo
    ) -> datetime.date:
        """Validates the start and end dates of the data model."""
        if info.data["start_date"] > end_date:
            raise ValueError(
                f"start date {info.data['start_date']} must be before or equal to end date {end_date}."
            )
        return end_date

    @property
    def dates(self) -> list[datetime.date]:
        import polars as pl

        return pl.date_range(
            self.start_date, self.end_date, interval="1d", eager=True
        ).to_list()

    def is_date_range(self) -> bool:
        return self.start_date != self.end_date

    def __str__(self) -> str:
        if self.start_date == self.end_date:
            return ":".join([super().__str__(), str(self.start_date)])
        else:
            return ":".join(
                [
                    super().__str__(),
                    "(from)" + str(self.start_date),
                    "(to)" + str(self.end_date),
                ]
            )
