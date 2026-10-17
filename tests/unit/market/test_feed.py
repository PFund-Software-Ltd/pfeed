from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from types import FunctionType

    from pfund.entities.products.product_base import BaseProduct
    from pytest_mock import MockerFixture, MockType

    from pfeed.market.data_model import MarketDataModel

import datetime
import threading
from types import SimpleNamespace

import polars as pl
import pytest
from pfund.datas.resolution import Resolution
from pfund.enums.env import Environment
from polars.testing import assert_frame_equal

from pfeed.enums import DataCategory, DataLayer, MarketDataType as DataType
from pfeed.io.base_io import BaseIO
from pfeed.market.feed import MarketFeed
from pfeed.source import BaseSource


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


@pytest.fixture
def source(mocker: MockerFixture, product: BaseProduct) -> MockType:
    """Stand-in for the feed's source: isinstance-compatible with BaseSource, records create_product() calls."""
    source = mocker.Mock(spec=BaseSource)
    source.name = "TEST"
    source.data_categories = {DataCategory.MARKET_DATA: {DataType.TICK: [], DataType.MINUTE: [], DataType.DAY: []}}
    source.Feeds = {DataCategory.MARKET_DATA: MarketFeed}
    source.create_product.return_value = product
    return source


@pytest.fixture
def feed(mocker: MockerFixture, source: MockType) -> MarketFeed:
    """A real MarketFeed on the fake source, with run() patched so download() stops before executing dataflows."""
    mocker.patch.object(MarketFeed, "DataSource", BaseSource, create=True)
    mocker.patch.object(MarketFeed, "download_dataflow_per_date", True, create=True)
    feed = MarketFeed(source)
    mocker.patch.object(feed, "run")
    return feed


def test_download(mocker: MockerFixture, feed: MarketFeed, source: MockType, product: BaseProduct):
    basis = str(product.basis)
    resolution = Resolution("1m")
    start_date, end_date = datetime.date(2025, 1, 1), datetime.date(2025, 1, 3)
    # non-default values, so a dropped argument can't pass by falling back to the request's own default
    data_layer, data_origin = DataLayer.RAW, "ORIGIN"
    io = mocker.Mock(spec=BaseIO)

    feed.download(
        basis,
        resolution,
        start_date=start_date,
        end_date=end_date,
        data_origin=data_origin,
        data_layer=data_layer,
        io=io,
        symbol=product.symbol,
    )

    source.create_product.assert_called_once_with(basis, symbol=product.symbol)
    [request] = feed._requests
    assert request.product is product
    assert request.env == Environment.BACKTEST
    assert request.target_resolution == resolution
    assert request.data_resolution == resolution
    assert request.start_date == start_date
    assert request.end_date == end_date
    assert request.data_layer == data_layer
    assert request.io is io
    assert request.data_origin == data_origin


def test_download_rolls_back_from_yesterday(feed: MarketFeed, product: BaseProduct):
    feed.download(str(product.basis), "1m", rollback_period="3d")

    [request] = feed._requests
    yesterday = datetime.datetime.now(datetime.UTC).date() - datetime.timedelta(days=1)
    assert request.start_date == yesterday - datetime.timedelta(days=2)
    assert request.end_date == yesterday


@pytest.mark.parametrize(
    ("target_resolution", "data_resolution"),
    [
        ("1t", "1t"),  # supported natively
        ("1s", "1t"),  # finer than every bar, only ticks can produce it
        ("5m", "1m"),  # the coarsest supported resolution that can produce it
        ("1h", "1m"),
        ("1d", "1d"),
    ],
)
def test_download_picks_data_resolution(
    feed: MarketFeed, product: BaseProduct, target_resolution: str, data_resolution: str
):
    feed.download(str(product.basis), target_resolution, start_date=datetime.date(2025, 1, 1))

    [request] = feed._requests
    assert request.target_resolution == Resolution(target_resolution)
    assert request.data_resolution == Resolution(data_resolution)


def test_download_unsupported_resolution(feed: MarketFeed, source: MockType, product: BaseProduct):
    source.data_categories = {DataCategory.MARKET_DATA: {DataType.DAY: []}}

    with pytest.raises(ValueError, match="is not supported by"):
        feed.download(str(product.basis), "1m", start_date=datetime.date(2025, 1, 1))

    assert feed._requests == []


@pytest.mark.parametrize("per_date", [True, False])
def test_download_dataflow_per_date(mocker: MockerFixture, feed: MarketFeed, product: BaseProduct, per_date: bool):
    mocker.patch.object(MarketFeed, "download_dataflow_per_date", per_date)
    start_date, end_date = datetime.date(2025, 1, 1), datetime.date(2025, 1, 3)

    feed.download(str(product.basis), "1m", start_date=start_date, end_date=end_date)

    dataflows = feed.dataflows
    assert len(dataflows) == (3 if per_date else 1)
    data_models = [cast("MarketDataModel", df.data_model) for df in dataflows]
    date_ranges = [(dm.start_date, dm.end_date) for dm in data_models]
    if per_date:
        dates = [start_date + datetime.timedelta(days=i) for i in range(3)]
        assert date_ranges == [(d, d) for d in dates]
    else:
        assert date_ranges == [(start_date, end_date)]


def test_download_returns_run_result(mocker: MockerFixture, feed: MarketFeed, product: BaseProduct):
    result = object()
    run = mocker.patch.object(feed, "run", return_value=result)

    assert feed.download(str(product.basis), "1m", start_date=datetime.date(2025, 1, 1)) is result
    run.assert_called_once_with()


def test_download_in_pipeline_mode_returns_feed(mocker: MockerFixture, feed: MarketFeed, product: BaseProduct):
    mocker.patch.object(feed, "_pipeline_mode", True)
    run = mocker.patch.object(feed, "run")

    assert feed.download(str(product.basis), "1m", start_date=datetime.date(2025, 1, 1)) is feed
    run.assert_not_called()


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


def _retrieve_transformations(product: BaseProduct, *, data_layer: DataLayer) -> list[FunctionType]:
    """Builds the retrieve transformations of batch data without running the feed's __init__, see _download_transformations()."""
    feed = _TransformationsFeed.__new__(_TransformationsFeed)
    feed.__dict__["_lock"] = threading.Lock()
    request = SimpleNamespace(
        env=Environment.BACKTEST,
        data_layer=data_layer,
        product=product,
        data_resolution=Resolution("1t"),
        target_resolution=Resolution("1m"),
        lock=threading.Lock(),
    )
    transformations = feed._get_default_transformations_for_retrieve(cast("Any", request))
    return cast("list[FunctionType]", transformations)


@pytest.mark.parametrize(
    ("data_layer", "expected"),
    [
        (DataLayer.CLEANED, ["resample_data_if_necessary", "_organize_columns"]),
        # raw and curated data have no known schema, returned as stored
        (DataLayer.RAW, []),
        (DataLayer.CURATED, []),
    ],
)
def test_get_default_transformations_for_retrieve(data_layer: DataLayer, expected: list[str], product: BaseProduct):
    transformations = _retrieve_transformations(product, data_layer=data_layer)
    assert [f.__name__ for f in transformations] == expected


def test_get_default_transformations_for_retrieve_resamples_to_target_resolution(product: BaseProduct):
    dates = [datetime.datetime(2025, 1, 1, 0, 0, s) for s in range(4)]
    lf = pl.LazyFrame({
        "date": dates,
        "product": [product.name] * 4,
        "resolution": [str(Resolution("1t"))] * 4,
        "price": [1.0, 3.0, 0.5, 2.0],
        "volume": [1.0] * 4,
    })
    for transformation in _retrieve_transformations(product, data_layer=DataLayer.CLEANED):
        lf = transformation(lf)
    out = lf.collect()
    assert out["resolution"].to_list() == [str(Resolution("1m"))]
    # key columns are moved to the leftmost side after resampling
    assert out.columns[:3] == ["date", "product", "resolution"]


def test_get_default_transformations_for_retrieve_picklable_without_feed(product: BaseProduct):
    cloudpickle = pytest.importorskip("cloudpickle")
    for transformation in _retrieve_transformations(product, data_layer=DataLayer.CLEANED):
        cloudpickle.dumps(transformation)


@pytest.mark.parametrize("extract_type", ["download", "retrieve", "stream"])
def test_get_default_transformations_dispatches_by_extract_type(mocker: MockerFixture, extract_type: str):
    feed = _TransformationsFeed.__new__(_TransformationsFeed)
    method = mocker.patch.object(feed, f"_get_default_transformations_for_{extract_type}", return_value=["t"])
    request = cast("Any", SimpleNamespace(extract_type=extract_type))

    assert feed._get_default_transformations(request) == ["t"]
    method.assert_called_once_with(request)


def test_get_default_transformations_unknown_extract_type():
    feed = _TransformationsFeed.__new__(_TransformationsFeed)
    with pytest.raises(ValueError, match="Unknown extract type: unknown"):
        feed._get_default_transformations(cast("Any", SimpleNamespace(extract_type="unknown")))


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


def test_standardize_columns(product: BaseProduct):
    lf = pl.LazyFrame({"date": [datetime.datetime(2025, 1, 1)], "price": [1.0]})
    out = MarketFeed._standardize_columns(lf, product, Resolution("1m")).collect()
    assert out.columns == ["date", "price", "product", "resolution"]
    assert out["product"].to_list() == [product.name]
    # str(resolution), not repr(resolution)
    assert out["resolution"].to_list() == [str(Resolution("1m"))]


def _key_cols(product: BaseProduct, resolution: str) -> dict[str, list[Any]]:
    return {
        "date": [datetime.datetime(2025, 1, 1)],
        "product": [product.name],
        "resolution": [str(Resolution(resolution))],
    }


def test_filter_columns_ticks_drops_vendor_columns(product: BaseProduct):
    lf = pl.LazyFrame({
        "vendor_col": ["x"],
        **_key_cols(product, "1t"),
        "side": pl.Series([1], dtype=pl.Int8),
        "volume": [2.0],
        "price": [1.0],
    })
    out = MarketFeed._filter_columns(lf, product).collect()
    # standard columns only, in the standard order
    assert out.columns == ["date", "product", "resolution", "price", "volume", "side"]
    assert out["side"].to_list() == [1]


def test_filter_columns_ticks_adds_missing_side(product: BaseProduct):
    lf = pl.LazyFrame({**_key_cols(product, "1t"), "price": [1.0], "volume": [2.0]})
    out = MarketFeed._filter_columns(lf, product).collect()
    # side is a standard column even if the source has no trade direction, null means unknown
    assert out.columns == ["date", "product", "resolution", "price", "volume", "side"]
    assert out.schema["side"] == pl.Int8
    assert out["side"].to_list() == [None]


def test_filter_columns_bars_drops_vendor_columns(product: BaseProduct):
    lf = pl.LazyFrame({
        **_key_cols(product, "1m"),
        "vendor_col": ["x"],
        "volume": [5.0],
        "close": [4.0],
        "low": [3.0],
        "high": [2.0],
        "open": [1.0],
    })
    out = MarketFeed._filter_columns(lf, product).collect()
    assert out.columns == ["date", "product", "resolution", "open", "high", "low", "close", "volume"]
    assert out.row(0)[3:] == (1.0, 2.0, 3.0, 4.0, 5.0)


def test_organize_columns_moves_key_columns_left(product: BaseProduct):
    lf = pl.LazyFrame({"price": [1.0], "resolution": ["1t"], "volume": [2.0], "date": [datetime.datetime(2025, 1, 1)], "product": [product.name]})
    out = MarketFeed._organize_columns(lf)
    # the other columns keep their relative order
    assert out.collect_schema().names() == ["date", "product", "resolution", "price", "volume"]


def test_organize_columns_returns_same_frame_when_already_organized(product: BaseProduct):
    lf = pl.LazyFrame({**_key_cols(product, "1t"), "price": [1.0]})
    assert MarketFeed._organize_columns(lf) is lf


@pytest.mark.skip(
    reason="TODO: stock schema is a draft; test dividends sum / splits product once stable"
)
def test_resample_data_stock_dividends_and_splits(): ...


@pytest.mark.skip(
    reason="TODO: stock schema is a draft; test dividends/splits columns are kept once stable"
)
def test_filter_columns_stock_dividends_and_splits(): ...
