from __future__ import annotations

import datetime

import pandera.errors
import polars as pl
import pytest
from pfund_blocks.resolution import Resolution

from pfeed.schemas.bar_data_schema import CLOSE, HIGH, LOW, OPEN, BarDataSchema
from pfeed.schemas.market_data_schema import PRODUCT, RESOLUTION, VOLUME
from pfeed.schemas.time_based_data_schema import DATE

START = datetime.datetime(2025, 1, 1)
MINUTE = 60
DAY = 24 * 60 * MINUTE


def _df(
    seconds: list[int] | None = None,
    resolution: str = "1m",
    start: datetime.datetime = START,
    values: dict[str, float] | None = None,
) -> pl.DataFrame:
    """Bars at `seconds` after `start`; `values` overrides a column for every bar, e.g. {OPEN: 4.0}."""
    if seconds is None:
        seconds = [0, MINUTE, 2 * MINUTE]
    n = len(seconds)
    columns = {OPEN: 2.0, HIGH: 3.0, LOW: 1.0, CLOSE: 2.0, VOLUME: 1.0} | (values or {})
    return pl.DataFrame(
        {
            DATE: pl.Series(
                [start + datetime.timedelta(seconds=s) for s in seconds],
                dtype=pl.Datetime("ns"),
            ),
            PRODUCT: pl.Series(["BYBIT_BTC_USDT_PERPETUAL"] * n, dtype=pl.String),
            RESOLUTION: pl.Series([str(Resolution(resolution))] * n, dtype=pl.String),
            **{
                column: pl.Series([value] * n, dtype=pl.Float64)
                for column, value in columns.items()
            },
        }
    )


def _failures(df: pl.DataFrame) -> set[tuple[str, str]]:
    """(column, check) pairs that fail; lazy=True collects all failures instead of stopping at the first."""
    try:
        BarDataSchema.validate(df, lazy=True)
    except pandera.errors.SchemaErrors as e:
        return set(e.failure_cases.select("column", "check").iter_rows())
    return set()


class TestBarDataSchema:
    def test_valid(self):
        df = _df()
        assert BarDataSchema.validate(df).equals(df)

    def test_flat_bar_passes(self):
        BarDataSchema.validate(_df(values=dict.fromkeys([OPEN, HIGH, LOW, CLOSE], 1.0)))

    @pytest.mark.parametrize("column", [OPEN, HIGH, LOW, CLOSE, VOLUME])
    def test_missing_column_fails(self, column: str):
        with pytest.raises(pandera.errors.SchemaError):
            BarDataSchema.validate(_df().drop(column))

    @pytest.mark.parametrize("value", [0.0, -1.0])
    @pytest.mark.parametrize("column", [OPEN, HIGH, LOW, CLOSE])
    def test_non_positive_price_fails(self, column: str, value: float):
        assert (column, "greater_than(0)") in _failures(_df(values={column: value}))

    def test_zero_volume_fails(self):
        assert _failures(_df(values={VOLUME: 0.0})) == {(VOLUME, "greater_than(0)")}

    @pytest.mark.parametrize("column", [OPEN, HIGH, LOW, CLOSE, VOLUME])
    def test_inf_fails(self, column: str):
        assert (column, "less_than(inf)") in _failures(
            _df(values={column: float("inf")})
        )

    @pytest.mark.parametrize("column", [OPEN, HIGH, LOW, CLOSE, VOLUME])
    def test_nan_fails(self, column: str):
        assert (column, "not_nullable") in _failures(_df(values={column: float("nan")}))

    @pytest.mark.parametrize(
        ("prices", "check"),
        [
            ({OPEN: 4.0}, "validate_high_is_highest"),
            ({CLOSE: 4.0}, "validate_high_is_highest"),
            ({OPEN: 0.5}, "validate_low_is_lowest"),
            ({CLOSE: 0.5}, "validate_low_is_lowest"),
            ({HIGH: 0.5, LOW: 3.0, OPEN: 2.0, CLOSE: 2.0}, "validate_high_is_highest"),
        ],
        ids=[
            "open_above_high",
            "close_above_high",
            "open_below_low",
            "close_below_low",
            "high_below_low",
        ],
    )
    def test_price_outside_high_low_fails(self, prices: dict[str, float], check: str):
        with pytest.raises(pandera.errors.SchemaError, match=check):
            BarDataSchema.validate(_df(values=prices))

    def test_duplicate_timestamps_fail(self):
        with pytest.raises(
            pandera.errors.SchemaError, match="validate_no_duplicate_timestamps"
        ):
            BarDataSchema.validate(_df(seconds=[0, MINUTE, MINUTE]))

    def test_inherits_market_data_checks(self):
        with pytest.raises(
            pandera.errors.SchemaError, match="date is not monotonic increasing"
        ):
            BarDataSchema.validate(_df(seconds=[MINUTE, 0]))

    @pytest.mark.parametrize("resolution", ["1t", "2t"], ids=["1_tick", "2_tick"])
    def test_tick_resolution_fails(self, resolution: str):
        # e.g. bars resampled from ticks but still labeled 1_TICK
        with pytest.raises(
            pandera.errors.SchemaError, match="resolution is not a bar resolution"
        ):
            BarDataSchema.validate(_df(resolution=resolution))


class TestDateGapsAreMultiplesOfResolution:
    @pytest.mark.parametrize(
        ("resolution", "seconds"),
        [
            ("1m", [0, MINUTE, 2 * MINUTE]),
            ("1m", [0, 3 * MINUTE, 13 * MINUTE]),  # missing bars
            ("2m", [0, 2 * MINUTE, 6 * MINUTE]),
            ("1d", [0, DAY, 3 * DAY]),
            ("1m", [0]),
        ],
        ids=["contiguous", "missing_bars", "2m", "1d", "single_bar"],
    )
    def test_whole_multiples_pass(self, resolution: str, seconds: list[int]):
        BarDataSchema.validate(_df(seconds, resolution))

    def test_offset_bars_pass(self):
        BarDataSchema.validate(
            _df(
                resolution="1h",
                start=START + datetime.timedelta(minutes=30),
                seconds=[0, 3600],
            )
        )

    @pytest.mark.parametrize(
        ("resolution", "seconds"),
        [
            ("1m", [0, 90]),
            ("2m", [0, MINUTE, 2 * MINUTE]),  # 1m bars labeled 2_MINUTE
            ("1d", [0, DAY // 2]),
        ],
        ids=["90s_gap_in_1m", "1m_bars_labeled_2m", "12h_gap_in_1d"],
    )
    def test_not_whole_multiples_fail(self, resolution: str, seconds: list[int]):
        with pytest.raises(
            pandera.errors.SchemaError,
            match="date gaps are not whole multiples of the resolution",
        ):
            BarDataSchema.validate(_df(seconds, resolution))

    def test_coarser_bars_labeled_finer_pass(self):
        # known blind spot: 2m bars labeled 1_MINUTE look like 1m bars trading every other minute
        BarDataSchema.validate(_df([0, 2 * MINUTE, 4 * MINUTE], "1m"))

    @pytest.mark.xfail(
        reason="pfund doesn't support resolutions lower than day, Resolution.is_bar() is False for them",
        strict=True,
    )
    @pytest.mark.parametrize(
        ("resolution", "seconds"),
        [("1mo", [0, 31 * DAY, 59 * DAY]), ("1y", [0, 365 * DAY, 731 * DAY])],
    )
    def test_month_and_year_are_skipped(self, resolution: str, seconds: list[int]):
        BarDataSchema.validate(_df(seconds, resolution))

    def test_empty_passes(self):
        BarDataSchema.validate(_df(seconds=[]))
