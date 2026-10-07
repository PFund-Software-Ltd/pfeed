from typing import cast

from types import SimpleNamespace

import pytest
from pfund.datas.resolution import Resolution

from pfeed.enums import DataCategory, MarketDataType as DataType
from pfeed.feeds.market_feed import MarketFeed


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
