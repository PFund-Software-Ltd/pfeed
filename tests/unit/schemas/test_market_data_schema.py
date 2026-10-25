from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfund_blocks.entities.products.product_base import BaseProduct

import datetime

import pandera.errors
import polars as pl
import pytest
from pfund_blocks.resolution import Resolution

from pfeed.schemas.bar_data_schema import BarDataSchema
from pfeed.schemas.market_data_schema import (
    PRODUCT,
    RESOLUTION,
    MarketDataSchema,
    get_market_data_schema,
)
from pfeed.schemas.tick_data_schema import TickDataSchema
from pfeed.schemas.time_based_data_schema import DATE

START = datetime.datetime(2025, 1, 1)


def _df(products: list[str | None], resolutions: list[str], dates: list[datetime.datetime] | None = None) -> pl.DataFrame:
    if dates is None:
        dates = [START + datetime.timedelta(seconds=i) for i in range(len(products))]
    return pl.DataFrame(
        {
            DATE: pl.Series(dates, dtype=pl.Datetime("ns")),
            PRODUCT: pl.Series(products, dtype=pl.String),
            RESOLUTION: resolutions,
        }
    )


class TestMarketDataSchema:
    def test_valid(self):
        df = _df(["BYBIT_BTC_USDT_PERPETUAL"] * 3, ["1_MINUTE"] * 3)
        assert MarketDataSchema.validate(df).equals(df)

    @pytest.mark.parametrize(
        "resolution",
        ["1t", "1q_L1", "1q_L2", "1q_L3", "1s", "1m", "1h", "1d", "5s", "2m", "15m", "60m", "4h", "2d"],
    )
    def test_str_resolutions_pass(self, resolution: str):
        MarketDataSchema.validate(_df(["BYBIT_BTC_USDT_PERPETUAL"], [str(Resolution(resolution))]))

    @pytest.mark.parametrize("resolution", ["1m", "2m", "MINUTE", "2_minute", "0_MINUTE", "foo"])
    def test_resolution_not_in_str_format_fails(self, resolution: str):
        with pytest.raises(pandera.errors.SchemaError):
            MarketDataSchema.validate(_df(["BYBIT_BTC_USDT_PERPETUAL"], [resolution]))

    def test_null_product_fails(self):
        with pytest.raises(pandera.errors.SchemaError):
            MarketDataSchema.validate(_df([None, None], ["1_MINUTE"] * 2))

    def test_multiple_resolutions_fail(self):
        with pytest.raises(pandera.errors.SchemaError, match="validate_unique_resolution"):
            MarketDataSchema.validate(_df(["BYBIT_BTC_USDT_PERPETUAL"] * 2, ["1_MINUTE", "1_HOUR"]))

    def test_multiple_products_fail(self):
        with pytest.raises(pandera.errors.SchemaError, match="validate_unique_product"):
            MarketDataSchema.validate(_df(["BYBIT_BTC_USDT_PERPETUAL", "BYBIT_ETH_USDT_PERPETUAL"], ["1_MINUTE"] * 2))

    @pytest.mark.parametrize("column", [PRODUCT, RESOLUTION])
    def test_missing_column_fails(self, column: str):
        with pytest.raises(pandera.errors.SchemaError):
            MarketDataSchema.validate(_df(["BYBIT_BTC_USDT_PERPETUAL"], ["1_MINUTE"]).drop(column))

    def test_inherits_date_check(self):
        dates = [START + datetime.timedelta(seconds=1), START]
        with pytest.raises(pandera.errors.SchemaError, match="date is not monotonic increasing"):
            MarketDataSchema.validate(_df(["BYBIT_BTC_USDT_PERPETUAL"] * 2, ["1_MINUTE"] * 2, dates))


class TestGetMarketDataSchema:
    @pytest.mark.parametrize(
        ("resolution", "expected"),
        [
            ("1t", TickDataSchema),
            ("1s", BarDataSchema),
            ("1m", BarDataSchema),
            ("1h", BarDataSchema),
            ("1d", BarDataSchema),
            ("5s", BarDataSchema),
            ("2m", BarDataSchema),
            ("4h", BarDataSchema),
            ("2d", BarDataSchema),
        ],
    )
    def test_tick_and_bar_resolutions(self, product: BaseProduct, resolution: str, expected: type[MarketDataSchema]):
        assert get_market_data_schema(product, Resolution(resolution)) is expected

    def test_quote_not_supported(self, product: BaseProduct):
        with pytest.raises(NotImplementedError):
            get_market_data_schema(product, Resolution("1q_L1"))
