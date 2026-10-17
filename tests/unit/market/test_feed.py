from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from types import FunctionType

    from pfund.entities.products.product_base import BaseProduct
    from pytest_mock import MockerFixture, MockType

    from pfeed.source import BaseSource

import datetime
import threading
from types import SimpleNamespace

import polars as pl
import pytest
from pfund.datas.resolution import Resolution
from pfund.enums.env import Environment
from polars.testing import assert_frame_equal

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


def _bars(
    resolution: str, dates: list[datetime.datetime], product: BaseProduct
) -> pl.LazyFrame:
    """Fake bar data with distinct values per row, so first/last/max/min/sum are distinguishable."""
    n = len(dates)
    return pl.LazyFrame(
        {
            "date": dates,
            "product": [str(product)] * n,
            "resolution": [str(Resolution(resolution))] * n,
            "open": [float(i + 1) for i in range(n)],
            "high": [float(10 * (i + 1)) for i in range(n)],
            "low": [0.5 * (i + 1) for i in range(n)],
            "close": [float(i + 5) for i in range(n)],
            "volume": [100.0 * (i + 1) for i in range(n)],
        }
    )


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
    """Stand-in for a feed class: create_data_model() only reads DataSource.metadata.name/create_product().

    Returns the feed class and its source's create_product mock.
    """
    create_product = mocker.Mock(return_value=product)
    data_source = SimpleNamespace(
        metadata=SimpleNamespace(name="TEST"), create_product=create_product
    )

    class FakeMarketFeed(MarketFeed):
        class Requests(MarketFeed.Requests):
            download = None
            retrieve = None
            stream = None
        DataSource = cast("type[BaseSource]", data_source)

    return FakeMarketFeed, create_product


def test_create_data_model_from_basis(mocker: MockerFixture, product: BaseProduct):
    Feed, create_product = _fake_feed_with_product(mocker, product)
    data_model = Feed.create_data_model(
        str(product.basis),
        "1m",
        "2025-01-01",
        "2025-01-03",
        symbol=product.symbol,
    )
    create_product.assert_called_once_with(str(product.basis), symbol=product.symbol)
    assert data_model.product is product
    assert data_model.data_source == "TEST"
    assert data_model.data_origin == "TEST"
    assert data_model.env == Environment.BACKTEST
    assert data_model.resolution == Resolution("1m")
    assert data_model.start_date == datetime.date(2025, 1, 1)
    assert data_model.end_date == datetime.date(2025, 1, 3)


def test_create_data_model_from_product_instance(
    mocker: MockerFixture, product: BaseProduct
):
    Feed, create_product = _fake_feed_with_product(mocker, product)
    data_model = Feed.create_data_model(
        product,
        DataType.TICK,
        datetime.date(2025, 1, 1),
        env="live",
        data_origin="ORIGIN",
    )
    create_product.assert_not_called()
    assert data_model.product is product
    assert data_model.data_origin == "ORIGIN"
    assert data_model.env == Environment.LIVE
    assert data_model.resolution == Resolution("1t")
    # end_date defaults to start_date: a single-day model
    assert data_model.end_date == data_model.start_date == datetime.date(2025, 1, 1)


class _TransformationsFeed(MarketFeed):
    # module-level so cloudpickle pickles it by reference, like a real feed class
    class Requests(MarketFeed.Requests):
        download = None
        retrieve = None
        stream = None


def _download_transformations(
    product: BaseProduct, *, should_clean_data: bool
) -> list[FunctionType]:
    """Builds the download transformations without running the feed's __init__.

    The feed and the request both hold a lock, which can't be pickled,
    so pickling a transformation fails if it captures either of them.
    """
    feed = _TransformationsFeed.__new__(_TransformationsFeed)
    # unpicklable, never read: pickling a transformation fails if it captures the feed
    feed.__dict__["_lock"] = threading.Lock()
    request = SimpleNamespace(
        should_clean_data=lambda: should_clean_data,
        product=product,
        data_resolution=Resolution("1t"),
        target_resolution=Resolution("1m"),
        # unpicklable, never read: pickling a transformation fails if it captures the request
        lock=threading.Lock(),
    )
    transformations = feed._get_default_transformations_for_download(cast("Any", request))
    # lambdas and staticmethods accessed via the class, all plain functions with a __name__
    return cast("list[FunctionType]", transformations)


@pytest.mark.parametrize(
    ("should_clean_data", "expected"),
    [
        # raw data stays a faithful mirror of the source, only its date column is standardized
        (False, ["standardize_date_column"]),
        (
            True,
            [
                "standardize_date_column",
                "_normalize_downloaded_data",
                "standardize_columns",
                "resample_data_if_necessary",
                "_organize_columns",
            ],
        ),
    ],
)
def test_get_default_transformations_for_download(
    should_clean_data: bool, expected: list[str], product: BaseProduct
):
    transformations = _download_transformations(product, should_clean_data=should_clean_data)
    assert [f.__name__ for f in transformations] == expected


def test_get_default_transformations_for_download_picklable_without_feed(product: BaseProduct):
    # dataflows (and their transformations) are pickled to Ray workers,
    # the transformations must not capture the feed or the request
    cloudpickle = pytest.importorskip("cloudpickle")
    for transformation in _download_transformations(product, should_clean_data=True):
        cloudpickle.dumps(transformation)


@pytest.mark.parametrize(
    ("df_resolution", "target"),
    [
        ("1m", "1m"),  # same resolution
        ("1h", "1m"),  # finer target: can't resample down
    ],
)
def test_resample_data_noop_when_target_not_coarser(
    df_resolution: str, target: str, product: BaseProduct
):
    dates = [datetime.datetime(2025, 1, 1, h) for h in range(4)]
    lf = _bars(df_resolution, dates, product)
    out = MarketFeed._resample_data(lf, product, target)
    assert_frame_equal(out.collect(), lf.collect())


def test_resample_data_bars(product: BaseProduct):
    dates = [datetime.datetime(2025, 1, 1, 0, m) for m in range(4)]
    lf = _bars("1m", dates, product)
    out = MarketFeed._resample_data(lf, product, "1h").collect()
    assert out.height == 1
    assert out.row(0, named=True) == {
        "date": datetime.datetime(2025, 1, 1, 0, 0),
        "open": 1.0,  # first
        "high": 40.0,  # max
        "low": 0.5,  # min
        "close": 8.0,  # last
        "volume": 1000.0,  # sum
        "product": str(product),
        "n_data_points": 4,
        "resolution": str(Resolution("1h")),
    }


def test_resample_data_bars_multiple_windows(product: BaseProduct):
    # 2 rows in the 00:00 hour, 2 rows in the 01:00 hour
    dates = [
        datetime.datetime(2025, 1, 1, 0, 58),
        datetime.datetime(2025, 1, 1, 0, 59),
        datetime.datetime(2025, 1, 1, 1, 0),
        datetime.datetime(2025, 1, 1, 1, 1),
    ]
    lf = _bars("1m", dates, product)
    out = MarketFeed._resample_data(lf, product, "1h").collect()
    # each bar aggregates only its own rows
    assert out.select("date", "open", "high", "low", "close", "volume").rows() == [
        (datetime.datetime(2025, 1, 1, 0, 0), 1.0, 20.0, 0.5, 6.0, 300.0),
        (datetime.datetime(2025, 1, 1, 1, 0), 3.0, 40.0, 1.5, 8.0, 700.0),
    ]


def test_resample_data_bars_unsorted_input(product: BaseProduct):
    # _bars assigns open=1,2,3,4 and close=5,6,7,8 by row position, not by date
    dates = [
        datetime.datetime(2025, 1, 1, 0, 3),  # open=1, close=5
        datetime.datetime(2025, 1, 1, 0, 1),  # open=2, close=6
        datetime.datetime(2025, 1, 1, 0, 0),  # open=3, close=7  <- earliest
        datetime.datetime(2025, 1, 1, 0, 2),  # open=4, close=8
    ]
    lf = _bars("1m", dates, product)
    out = MarketFeed._resample_data(lf, product, "1h").collect()
    # open/close come from the earliest/latest timestamp, not the first/last row
    assert out["open"].to_list() == [3.0]
    assert out["close"].to_list() == [5.0]


def test_resample_data_ticks(product: BaseProduct):
    t = datetime.datetime(2025, 1, 1, 0, 0)
    lf = pl.LazyFrame(
        {
            "date": [
                t + datetime.timedelta(seconds=10),  # 00:00 window
                t + datetime.timedelta(seconds=40),
                t + datetime.timedelta(seconds=50),
                t + datetime.timedelta(seconds=65),  # 00:01 window
                t + datetime.timedelta(seconds=90),
            ],
            "product": [str(product)] * 5,
            "resolution": [str(Resolution("1t"))] * 5,
            "price": [100.0, 105.0, 99.0, 101.0, 103.0],
            "volume": [1.0, 2.0, 3.0, 4.0, 5.0],
            "side": pl.Series([1, -1, 1, -1, 1], dtype=pl.Int8),
        }
    )
    out = MarketFeed._resample_data(lf, product, "1m").collect()
    # price becomes OHLC, volume is summed, side is dropped
    assert "price" not in out.columns
    assert "side" not in out.columns
    assert out.select(
        "date", "open", "high", "low", "close", "volume", "n_data_points"
    ).rows() == [
        (datetime.datetime(2025, 1, 1, 0, 0), 100.0, 105.0, 99.0, 99.0, 6.0, 3),
        (datetime.datetime(2025, 1, 1, 0, 1), 101.0, 103.0, 101.0, 103.0, 9.0, 2),
    ]


def _dates(
    start: datetime.datetime, step: datetime.timedelta, n: int
) -> list[datetime.datetime]:
    return [start + step * i for i in range(n)]


@pytest.mark.parametrize(
    ("df_resolution", "target", "dates", "expected_dates", "expected_n_data_points"),
    [
        pytest.param(
            "1m",
            "2m",
            _dates(datetime.datetime(2025, 1, 1), datetime.timedelta(minutes=1), 4),
            [datetime.datetime(2025, 1, 1, 0, 0), datetime.datetime(2025, 1, 1, 0, 2)],
            [2, 2],
            id="2m",
        ),
        pytest.param(
            "1h",
            "4h",
            _dates(datetime.datetime(2025, 1, 1), datetime.timedelta(hours=1), 8),
            [datetime.datetime(2025, 1, 1, 0), datetime.datetime(2025, 1, 1, 4)],
            [4, 4],
            id="4h",
        ),
        pytest.param(
            # 3d windows are aligned to the epoch: 2024-12-31 is a window start
            "1d",
            "3d",
            _dates(datetime.datetime(2024, 12, 31), datetime.timedelta(days=1), 6),
            [datetime.datetime(2024, 12, 31), datetime.datetime(2025, 1, 3)],
            [3, 3],
            id="3d",
        ),
        pytest.param(
            # 2w windows are aligned to the epoch: Mon 2025-01-06 is a window start
            "1w",
            "2w",
            _dates(datetime.datetime(2025, 1, 6), datetime.timedelta(weeks=1), 4),
            [datetime.datetime(2025, 1, 6), datetime.datetime(2025, 1, 20)],
            [2, 2],
            id="2w",
        ),
        pytest.param(
            # data not starting on a window start: edge windows are partial, not shifted
            "1d",
            "3d",
            _dates(datetime.datetime(2025, 1, 1), datetime.timedelta(days=1), 6),
            [
                datetime.datetime(2024, 12, 31),
                datetime.datetime(2025, 1, 3),
                datetime.datetime(2025, 1, 6),
            ],
            [2, 3, 1],
            id="3d-partial-edges",
        ),
    ],
)
def test_resample_data_multi_period(
    df_resolution: str,
    target: str,
    dates: list[datetime.datetime],
    expected_dates: list[datetime.datetime],
    expected_n_data_points: list[int],
    product: BaseProduct,
):
    lf = _bars(df_resolution, dates, product)
    out = MarketFeed._resample_data(lf, product, target).collect()
    assert out["date"].to_list() == expected_dates
    assert out["n_data_points"].to_list() == expected_n_data_points


@pytest.mark.parametrize(
    (
        "df_resolution",
        "target",
        "offset",
        "dates",
        "expected_dates",
        "expected_n_data_points",
    ),
    [
        pytest.param(
            # windows start at XX:30; rows before the first XX:30 fall into the previous window
            "1m",
            "1h",
            "30m",
            _dates(datetime.datetime(2025, 1, 1), datetime.timedelta(minutes=1), 120),
            [
                datetime.datetime(2024, 12, 31, 23, 30),
                datetime.datetime(2025, 1, 1, 0, 30),
                datetime.datetime(2025, 1, 1, 1, 30),
            ],
            [30, 60, 30],
            id="1h-offset-30m",
        ),
        pytest.param(
            # offset shifts the multi-period grid too: windows start at 01:00, 05:00, ...
            "1h",
            "4h",
            "1h",
            _dates(datetime.datetime(2025, 1, 1), datetime.timedelta(hours=1), 8),
            [
                datetime.datetime(2024, 12, 31, 21),
                datetime.datetime(2025, 1, 1, 1),
                datetime.datetime(2025, 1, 1, 5),
            ],
            [1, 4, 3],
            id="4h-offset-1h",
        ),
    ],
)
def test_resample_data_offset(
    df_resolution: str,
    target: str,
    offset: str,
    dates: list[datetime.datetime],
    expected_dates: list[datetime.datetime],
    expected_n_data_points: list[int],
    product: BaseProduct,
):
    lf = _bars(df_resolution, dates, product)
    out = MarketFeed._resample_data(lf, product, target, offset=offset).collect()
    assert out["date"].to_list() == expected_dates
    assert out["n_data_points"].to_list() == expected_n_data_points


@pytest.mark.skip(
    reason="TODO: stock schema is a draft; test dividends sum / splits product once stable"
)
def test_resample_data_stock_dividends_and_splits(): ...


@pytest.mark.skip(
    reason="TODO: stock schema is a draft; test dividends/splits columns are kept once stable"
)
def test_filter_columns_stock_dividends_and_splits(): ...
