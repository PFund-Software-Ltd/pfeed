from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from pfeed.source import BaseSource

import datetime
from types import SimpleNamespace

import polars as pl
import pytest
from pfund.datas.resolution import Resolution

from pfeed.base.time_based_data_model import TimeBasedDataModel
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
) -> type[TimeBasedFeed[Any, Any, Any]]:
    """Stand-in feed class: _standardize_dates() only reads DataSource.METADATA (name, start_date)."""
    metadata = SimpleNamespace(name="FAKE", start_date=source_start_date)

    class FakeFeed(TimeBasedFeed[Any, Any, Any]):
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


def _fake_feed_with_date_cols(
    *date_cols: str,
) -> type[TimeBasedFeed[Any, Any, Any]]:
    """Stand-in feed class: _standardize_date_column() only reads downloaded_data_date_cols and DataModel's date col names."""

    class FakeFeed(TimeBasedFeed[Any, Any, Any]):
        DataModel = TimeBasedDataModel
        downloaded_data_date_cols = list(date_cols)

    return FakeFeed


JAN_1 = datetime.datetime(2025, 1, 1)
JAN_1_NOON = datetime.datetime(2025, 1, 1, 12)


def test_standardize_date_column_cleaned_renames_source_col() -> None:
    feed = _fake_feed_with_date_cols("timestamp")
    lf = pl.LazyFrame({"timestamp": [JAN_1], "price": [1.0]})
    df = feed._standardize_date_column(lf, is_raw_data=False).collect()
    assert df.columns == ["date", "price"]


def test_standardize_date_column_raw_keeps_source_col() -> None:
    feed = _fake_feed_with_date_cols("timestamp")
    epoch_ms = 1735689600000  # 2025-01-01
    lf = pl.LazyFrame({"timestamp": [epoch_ms], "price": [1.0]})
    df = feed._standardize_date_column(lf, is_raw_data=True).collect()
    assert df.columns == ["timestamp", "price", "_pfeed_date"]
    # the source column stays a faithful mirror, only the added copy is converted
    assert df.schema["timestamp"] == pl.Int64
    assert df["timestamp"].to_list() == [epoch_ms]
    assert df["_pfeed_date"].to_list() == [JAN_1]


def test_standardize_date_column_uses_first_present_candidate() -> None:
    feed = _fake_feed_with_date_cols("Datetime", "Date")
    lf = pl.LazyFrame({"Date": [JAN_1], "Datetime": [JAN_1_NOON]})
    df = feed._standardize_date_column(lf, is_raw_data=False).collect()
    assert df["date"].to_list() == [JAN_1_NOON]
    assert "Date" in df.columns


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        # strings
        (["2025-01-01"], [JAN_1]),
        (["2025-01-01 12:00:00"], [JAN_1_NOON]),
        (["2025-01-01T20:00:00+08:00"], [JAN_1_NOON]),
        (["2025-01-01T12:00:00Z"], [JAN_1_NOON]),
        # epoch numbers, unit inferred from magnitude
        ([1735689600], [JAN_1]),
        ([1735689600_000], [JAN_1]),
        ([1735689600_000_000], [JAN_1]),
        ([1735689600_000_000_000], [JAN_1]),
        ([1735732800.5], [JAN_1_NOON + datetime.timedelta(milliseconds=500)]),
        # null epoch values don't break unit inference
        ([None, 1735689600], [None, JAN_1]),
        # datetimes, tz-aware ones converted to UTC
        ([JAN_1], [JAN_1]),
        ([JAN_1_NOON.replace(tzinfo=datetime.UTC)], [JAN_1_NOON]),
        (
            [datetime.datetime(2025, 1, 1, 20, tzinfo=datetime.timezone(datetime.timedelta(hours=8)))],
            [JAN_1_NOON],
        ),
        # dates, midnight of that date
        ([datetime.date(2025, 1, 1)], [JAN_1]),
    ],
)
def test_standardize_date_column_converts_to_ns_datetime(
    values: list[Any], expected: list[datetime.datetime]
) -> None:
    feed = _fake_feed_with_date_cols("ts")
    lf = pl.LazyFrame({"ts": values})
    df = feed._standardize_date_column(lf, is_raw_data=False).collect()
    assert df.schema["date"] == pl.Datetime("ns")
    assert df["date"].to_list() == expected


def test_standardize_date_column_sorts_by_date() -> None:
    feed = _fake_feed_with_date_cols("ts")
    lf = pl.LazyFrame({"ts": [JAN_1_NOON, JAN_1], "price": [2.0, 1.0]})
    df = feed._standardize_date_column(lf, is_raw_data=False).collect()
    assert df["date"].to_list() == [JAN_1, JAN_1_NOON]
    assert df["price"].to_list() == [1.0, 2.0]


@pytest.mark.parametrize(
    ("lf", "match"),
    [
        (pl.LazyFrame({"other": [JAN_1]}), "no date column"),
        (pl.LazyFrame({"ts": [True]}), "unsupported dtype"),
        (pl.LazyFrame({"ts": [datetime.time(12)]}), "unsupported dtype"),
        (pl.LazyFrame({"ts": [1735689600, 1735689600_000]}), "mixes epoch time units"),
    ],
)
def test_standardize_date_column_invalid(lf: pl.LazyFrame, match: str) -> None:
    feed = _fake_feed_with_date_cols("ts")
    with pytest.raises(ValueError, match=match):
        feed._standardize_date_column(lf, is_raw_data=False)


def test_standardize_date_column_empty_epoch_column() -> None:
    feed = _fake_feed_with_date_cols("ts")
    lf = pl.LazyFrame({"ts": pl.Series([], dtype=pl.Int64)})
    df = feed._standardize_date_column(lf, is_raw_data=False).collect()
    assert df.schema["date"] == pl.Datetime("ns")
    assert df.is_empty()
