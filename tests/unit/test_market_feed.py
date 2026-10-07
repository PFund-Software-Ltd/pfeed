from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct
    from pytest_mock import MockerFixture, MockType

    from pfeed.source import BaseSource

import datetime
from types import SimpleNamespace

import pytest
from pfund.datas.resolution import Resolution
from pfund.enums.env import Environment

from pfeed.enums import DataCategory, MarketDataType as DataType
from pfeed.market.feed import MarketFeed


def _fake_feed(*data_types: DataType) -> MarketFeed:
    """Stand-in for a feed: get_supported_resolutions() only reads data_domain and data_source.data_categories."""
    data_source = SimpleNamespace(
        data_categories={DataCategory.MARKET_DATA: {dt: [] for dt in data_types}}
    )
    return cast(
        "MarketFeed",
        SimpleNamespace(data_domain=MarketFeed.data_domain, data_source=data_source),
    )


def _resols(*resols: str) -> list[Resolution]:
    return [Resolution(r) for r in resols]


@pytest.mark.parametrize(
    ("data_types", "include_resampled", "expected"),
    [
        # native only: exactly what the source provides
        ((DataType.TICK,), False, _resols("1t")),
        ((DataType.MINUTE, DataType.DAY), False, _resols("1m", "1d")),
        # resampled: finest native + everything coarser, finest first
        (
            (DataType.TICK,),
            True,
            _resols("1t", "1s", "1m", "1h", "1d", "1w", "1mo", "1y"),
        ),
        # coarser native resolutions are not duplicated
        (
            (DataType.MINUTE, DataType.DAY),
            True,
            _resols("1m", "1h", "1d", "1w", "1mo", "1y"),
        ),
        # quotes pass through but are never a resampling base
        (
            (DataType.QUOTE_L1, DataType.HOUR),
            True,
            _resols("1q_L1", "1h", "1d", "1w", "1mo", "1y"),
        ),
        # quote-only source: no resampling base, native only
        ((DataType.QUOTE_L2,), True, _resols("1q_L2")),
        # no market data types
        ((), False, []),
        ((), True, []),
    ],
)
def test_get_supported_resolutions(
    data_types: tuple[DataType, ...],
    include_resampled: bool,
    expected: list[Resolution],
):
    feed = _fake_feed(*data_types)
    resolutions = MarketFeed.get_supported_resolutions(
        feed, include_resampled=include_resampled
    )
    assert resolutions == expected


def _fake_feed_with_product(
    mocker: MockerFixture, product: BaseProduct
) -> tuple[type[MarketFeed], MockType]:
    """Stand-in for a feed class: create_data_model() only reads DataSource.METADATA.name/create_product().

    Returns the feed class and its source's create_product mock.
    """
    create_product = mocker.Mock(return_value=product)
    data_source = SimpleNamespace(
        METADATA=SimpleNamespace(name="TEST"), create_product=create_product
    )

    class FakeMarketFeed(MarketFeed):
        capabilities = frozenset()
        DataSource = cast("type[BaseSource]", data_source)

    return FakeMarketFeed, create_product


def test_create_data_model_from_basis(
    mocker: MockerFixture, bybit_product: BaseProduct
):
    Feed, create_product = _fake_feed_with_product(mocker, bybit_product)
    data_model = Feed.create_data_model(
        str(bybit_product.basis),
        "1m",
        "2025-01-01",
        "2025-01-03",
        symbol=bybit_product.symbol,
    )
    create_product.assert_called_once_with(
        str(bybit_product.basis), symbol=bybit_product.symbol
    )
    assert data_model.product is bybit_product
    assert data_model.data_source == "TEST"
    assert data_model.data_origin == "TEST"
    assert data_model.env == Environment.BACKTEST
    assert data_model.resolution == Resolution("1m")
    assert data_model.start_date == datetime.date(2025, 1, 1)
    assert data_model.end_date == datetime.date(2025, 1, 3)


def test_create_data_model_from_product_instance(
    mocker: MockerFixture, bybit_product: BaseProduct
):
    Feed, create_product = _fake_feed_with_product(mocker, bybit_product)
    data_model = Feed.create_data_model(
        bybit_product,
        DataType.TICK,
        datetime.date(2025, 1, 1),
        env="live",
        data_origin="ORIGIN",
    )
    create_product.assert_not_called()
    assert data_model.product is bybit_product
    assert data_model.data_origin == "ORIGIN"
    assert data_model.env == Environment.LIVE
    assert data_model.resolution == Resolution("1t")
    # end_date defaults to start_date: a single-day model
    assert data_model.end_date == data_model.start_date == datetime.date(2025, 1, 1)
