import datetime

from pydantic import field_validator

from pfeed.requests.base_request import BaseRequest


class TimeBasedFeedBaseRequest(BaseRequest):
    start_date: datetime.date | str
    end_date: datetime.date | str

    @field_validator("start_date", mode="before")
    @classmethod
    def create_start_date(cls, v: datetime.date | str) -> datetime.date:
        if isinstance(v, str):
            return datetime.date.fromisoformat(v)
        return v
    
    @field_validator("end_date", mode="before")
    @classmethod
    def create_end_date(cls, v: datetime.date | str) -> datetime.date:
        if isinstance(v, str):
            return datetime.date.fromisoformat(v)
        return v