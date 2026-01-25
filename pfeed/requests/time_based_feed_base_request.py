import datetime

from pfeed.requests.base_request import BaseRequest


class TimeBasedFeedBaseRequest(BaseRequest):
    start_date: datetime.date
    end_date: datetime.date
    dataflow_per_date: bool
