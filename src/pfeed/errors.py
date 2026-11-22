from __future__ import annotations

import datetime


class PFeedError(Exception):
    pass


class DataNotFoundError(PFeedError):
    """Raised when requested data is not found in any storage backend"""

    pass


class MissingDatesError(DataNotFoundError):
    """Raised when requested data is only partially stored, e.g. by retrieve(raise_on_missing_data=True).

    Attributes:
        missing_dates: the requested dates that are not stored.
    """

    def __init__(self, message: str, missing_dates: list[datetime.date]):
        super().__init__(message)
        self.missing_dates = missing_dates
