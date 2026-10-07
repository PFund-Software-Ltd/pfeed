from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from pfeed.source import BaseSource

import datetime
from types import SimpleNamespace

import pytest
from pfund.datas.resolution import Resolution

from pfeed.base.time_based_feed import TimeBasedFeed

NOW = datetime.datetime(2025, 3, 15, 12, tzinfo=datetime.UTC)
YESTERDAY = datetime.date(2025, 3, 14)
DAILY = Resolution("1d")


@pytest.fixture(autouse=True)
def freeze_today(mocker: MockerFixture) -> None:
    mocker.patch("pfeed.utils.temporal.get_utc_now", return_value=NOW)
    mocker.patch("pfeed.utils.temporal.get_yesterday", return_value=YESTERDAY)


def _fake_feed(
    source_start_date: datetime.date | None = None,
) -> type[TimeBasedFeed[Any]]:
    """Stand-in feed class: _standardize_dates() only reads DataSource.METADATA (name, start_date)."""
    metadata = SimpleNamespace(name="FAKE", start_date=source_start_date)

    class FakeFeed(TimeBasedFeed[Any]):
        DataSource = cast("type[BaseSource]", SimpleNamespace(METADATA=metadata))

    return FakeFeed


@pytest.mark.parametrize(
    ("start_date", "end_date", "rollback_period", "expected"),
    [
        # explicit dates win over rollback_period, strings and dates both accepted
        (
            "2025-01-01",
            "2025-01-31",
            "7d",
            (datetime.date(2025, 1, 1), datetime.date(2025, 1, 31)),
        ),
        (
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 31),
            "7d",
            (datetime.date(2025, 1, 1), datetime.date(2025, 1, 31)),
        ),
        (
            "2025-01-01",
            "2025-01-01",
            "7d",
            (datetime.date(2025, 1, 1), datetime.date(2025, 1, 1)),
        ),
        # start_date alone runs up to yesterday
        ("2025-01-01", None, "7d", (datetime.date(2025, 1, 1), YESTERDAY)),
        # start_date given, 'max' is ignored like any other rollback_period
        ("2025-01-01", None, "max", (datetime.date(2025, 1, 1), YESTERDAY)),
        # no start_date, derived from rollback_period, ending yesterday
        (None, None, "1d", (YESTERDAY, YESTERDAY)),
        (None, None, "7d", (datetime.date(2025, 3, 8), YESTERDAY)),
        (None, None, Resolution("7d"), (datetime.date(2025, 3, 8), YESTERDAY)),
        (None, None, "ytd", (datetime.date(2025, 1, 1), YESTERDAY)),
        (None, None, "YTD", (datetime.date(2025, 1, 1), YESTERDAY)),
    ],
)
def test_standardize_dates(
    start_date: str | datetime.date | None,
    end_date: str | datetime.date | None,
    rollback_period: Resolution | str,
    expected: tuple[datetime.date, datetime.date],
) -> None:
    feed = _fake_feed()
    assert (
        feed._standardize_dates(DAILY, start_date, end_date, rollback_period)
        == expected
    )


@pytest.mark.parametrize("rollback_period", ["max", "MAX"])
def test_standardize_dates_max_uses_source_start_date(rollback_period: str) -> None:
    feed = _fake_feed(source_start_date=datetime.date(2020, 1, 1))
    assert feed._standardize_dates(DAILY, None, None, rollback_period) == (
        datetime.date(2020, 1, 1),
        YESTERDAY,
    )


def test_standardize_dates_max_delegates_to_hook_with_resolution(
    mocker: MockerFixture,
) -> None:
    feed = _fake_feed()
    expected = (datetime.date(2024, 3, 15), YESTERDAY)
    hook = mocker.patch.object(feed, "_max_date_range", return_value=expected)
    assert feed._standardize_dates(DAILY, None, None, "max") == expected
    hook.assert_called_once_with(DAILY)


@pytest.mark.parametrize(
    ("source_start_date", "start_date", "end_date", "rollback_period", "match"),
    [
        # end_date without start_date is ambiguous, rejected for 'max' too instead of silently dropped
        (datetime.date(2020, 1, 1), None, "2025-01-31", "max", "start_date is not"),
        (None, None, "2025-01-31", "7d", "start_date is not"),
        # 'max' needs the source to know its start_date
        (None, None, None, "max", "no data source start_date"),
        # start_date after end_date
        (None, "2025-02-01", "2025-01-01", "7d", "must be <="),
        # unknown rollback_period, rejected by pfund's Resolution
        (None, None, None, "abc", "Invalid resolution"),
    ],
)
def test_standardize_dates_invalid(
    source_start_date: datetime.date | None,
    start_date: str | None,
    end_date: str | None,
    rollback_period: str,
    match: str,
) -> None:
    feed = _fake_feed(source_start_date=source_start_date)
    with pytest.raises(ValueError, match=match):
        feed._standardize_dates(DAILY, start_date, end_date, rollback_period)
