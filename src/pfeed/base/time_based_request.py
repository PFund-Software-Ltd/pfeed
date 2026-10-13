from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.base.time_based_data_model import TimeBasedDataModel

import datetime

from pfeed.base.request import BaseRequest


class TimeBasedFeedBaseRequest(BaseRequest):
    start_date: datetime.date
    end_date: datetime.date

    def to_data_model(self) -> TimeBasedDataModel:
        raise NotImplementedError
