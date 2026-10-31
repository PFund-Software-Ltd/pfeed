from __future__ import annotations

import datetime

import pandera.errors
import polars as pl
import pytest
from pfund_blocks.resolution import Resolution

from pfeed.schemas.market_data_schema import PRODUCT, RESOLUTION, VOLUME
from pfeed.schemas.tick_data_schema import PRICE, SIDE, TickDataSchema
from pfeed.schemas.time_based_data_schema import DATE

START = datetime.datetime(2025, 1, 1)


def _df(
    seconds: list[int] | None = None,
    resolution: str = "1t",
    values: dict[str, float] | None = None,
    sides: list[int | None] | None = None,
) -> pl.DataFrame:
    """Ticks at `seconds` after START; `values` overrides a column for every tick, e.g. {PRICE: 0.0}."""
    if seconds is None:
        seconds = [0, 1, 2]
    n = len(seconds)
    columns = {PRICE: 2.0, VOLUME: 1.0} | (values or {})
    return pl.DataFrame(
        {
            DATE: pl.Series(
                [START + datetime.timedelta(seconds=s) for s in seconds],
                dtype=pl.Datetime("ns"),
            ),
            PRODUCT: pl.Series(["BYBIT_BTC_USDT_PERPETUAL"] * n, dtype=pl.String),
            RESOLUTION: pl.Series([str(Resolution(resolution))] * n, dtype=pl.String),
            **{
                column: pl.Series([value] * n, dtype=pl.Float64)
                for column, value in columns.items()
            },
            SIDE: pl.Series(
                sides if sides is not None else [1, -1, 1][:n], dtype=pl.Int8
            ),
        }
    )


def _failures(df: pl.DataFrame) -> set[tuple[str, str]]:
    """(column, check) pairs that fail; lazy=True collects all failures instead of stopping at the first."""
    try:
        TickDataSchema.validate(df, lazy=True)
    except pandera.errors.SchemaErrors as e:
        return set(e.failure_cases.select("column", "check").iter_rows())
    return set()


class TestTickDataSchema:
    def test_valid(self):
        df = _df()
        assert TickDataSchema.validate(df).equals(df)

    def test_duplicate_timestamps_pass(self):
        # several trades can share a timestamp
        TickDataSchema.validate(_df(seconds=[0, 1, 1]))

    @pytest.mark.parametrize("column", [PRICE, VOLUME, SIDE])
    def test_missing_column_fails(self, column: str):
        with pytest.raises(pandera.errors.SchemaError):
            TickDataSchema.validate(_df().drop(column))

    @pytest.mark.parametrize("value", [0.0, -1.0])
    def test_non_positive_price_passes(self, value: float):
        TickDataSchema.validate(_df(values={PRICE: value}))

    @pytest.mark.parametrize("value", [0.0, -1.0])
    def test_non_positive_volume_fails(self, value: float):
        assert (VOLUME, "greater_than(0)") in _failures(_df(values={VOLUME: value}))

    @pytest.mark.parametrize("column", [PRICE, VOLUME])
    def test_inf_fails(self, column: str):
        assert (column, "less_than(inf)") in _failures(
            _df(values={column: float("inf")})
        )

    @pytest.mark.parametrize("column", [PRICE, VOLUME])
    def test_nan_fails(self, column: str):
        assert (column, "not_nullable") in _failures(_df(values={column: float("nan")}))

    def test_inherits_market_data_checks(self):
        with pytest.raises(
            pandera.errors.SchemaError, match="date is not monotonic increasing"
        ):
            TickDataSchema.validate(_df(seconds=[1, 0]))


class TestSide:
    @pytest.mark.parametrize(
        "sides",
        [[1, 1, 1], [-1, -1, -1], [None, None, None], [1, None, -1]],
        ids=["buys_only", "sells_only", "all_null", "partly_null"],
    )
    def test_valid_sides_pass(self, sides: list[int | None]):
        # one-sided batches are valid, null means the source has no trade direction
        TickDataSchema.validate(_df(sides=sides))

    @pytest.mark.parametrize("side", [0, 2])
    def test_side_not_buy_or_sell_fails(self, side: int):
        assert (SIDE, "isin([1, -1])") in _failures(_df(sides=[1, side, -1]))

    def test_side_not_int8_fails(self):
        df = _df().with_columns(pl.col(SIDE).cast(pl.Int64))
        assert (SIDE, "dtype('Int8')") in _failures(df)


class TestResolution:
    def test_1_tick_passes(self):
        TickDataSchema.validate(_df(resolution="1t"))

    @pytest.mark.parametrize(
        "resolution", ["2t", "1s", "1m"], ids=["2_tick", "1_second", "1_minute"]
    )
    def test_not_1_tick_fails(self, resolution: str):
        assert (RESOLUTION, "equal_to(1_TICK)") in _failures(_df(resolution=resolution))
