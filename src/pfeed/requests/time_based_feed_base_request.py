from __future__ import annotations

from typing import TYPE_CHECKING

import datetime

if TYPE_CHECKING:
    from pfeed.data_models.time_based_data_model import TimeBasedDataModel

from pydantic import field_validator

from pfeed.requests.base_request import BaseRequest


class TimeBasedFeedBaseRequest(BaseRequest):
    start_date: datetime.date | str
    end_date: datetime.date | str

    @field_validator("start_date", mode="before")
    @classmethod
    def _validate_start_date(cls, v: datetime.date | str) -> datetime.date:
        if isinstance(v, str):
            return datetime.date.fromisoformat(v)
        return v

    @field_validator("end_date", mode="before")
    @classmethod
    def _validate_end_date(cls, v: datetime.date | str) -> datetime.date:
        if isinstance(v, str):
            return datetime.date.fromisoformat(v)
        return v

    def to_data_model(self) -> TimeBasedDataModel:
        raise NotImplementedError
