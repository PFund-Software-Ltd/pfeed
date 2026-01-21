import datetime

from pfeed.requests.base_request import BaseRequest
from pfeed.enums import ExtractType


class TimeBasedFeedDownloadRequest(BaseRequest):
    start_date: datetime.date
    end_date: datetime.date
    dataflow_per_date: bool

    request_type: ExtractType = ExtractType.download
