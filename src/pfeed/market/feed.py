from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, cast, overload

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Hashable

    from pfund_blocks.datas.bar_data import Bar, BarData
    from pfund_blocks.datas.market_data import MarketData
    from pfund_blocks.datas.tick_data import Tick
    from pfund_blocks.entities.products.product_base import BaseProduct
    from pfund_blocks.typing import Message, ProductName

    from pfeed.dataflow.result import RunResult
    from pfeed.market.data_handler import MarketDataHandler
    from pfeed.market.requests.base_request import MarketFeedBaseRequest
    from pfeed.source import BaseSource

import datetime
from abc import ABC

import polars as pl
from pfund_blocks.enums.env import Environment
from pfund_blocks.resolution import Resolution

from pfeed.base.feed import requires
from pfeed.base.time_based_feed import TimeBasedFeed
from pfeed.config import setup_logging
from pfeed.enums import DataCategory, DataLayer, MarketDataType
from pfeed.io.base_io import BaseIO
from pfeed.market.data_model import MarketDataModel
from pfeed.market.requests import (
    MarketFeedDownloadRequest,
    MarketFeedReplayRequest,
    MarketFeedRetrieveRequest,
    MarketFeedStreamRequest,
)
from pfeed.streaming.mixin import StreamingMixin


class MarketFeed[
    SourceT: BaseSource[bool],
    RequestT: MarketFeedBaseRequest = MarketFeedBaseRequest,
    DataModelT: MarketDataModel = MarketDataModel,
    PipelineT: bool = Literal[False],
](StreamingMixin, TimeBasedFeed[SourceT, RequestT, DataModelT, PipelineT], ABC):
    class Requests:  # ty: ignore[incompatible-attribute-override]
        """Request class per verb, a subclass sets a verb to None if it doesn't support it."""

        download: ClassVar[type[MarketFeedDownloadRequest] | None] = (
            MarketFeedDownloadRequest
        )
        retrieve: ClassVar[type[MarketFeedRetrieveRequest] | None] = (
            MarketFeedRetrieveRequest
        )
        stream: ClassVar[type[MarketFeedStreamRequest] | None] = MarketFeedStreamRequest
        replay: ClassVar[type[MarketFeedReplayRequest] | None] = MarketFeedReplayRequest

    DataModel: ClassVar[type[MarketDataModel]] = MarketDataModel  # ty: ignore[incompatible-mutable-override]
    data_domain: ClassVar[DataCategory] = DataCategory.MARKET_DATA
    _required_methods: ClassVar[dict[str, tuple[str, ...]]] = {
        MarketFeedDownloadRequest.extract_type: (
            "_download_impl",
            "_normalize_downloaded_data",
        ),
        MarketFeedStreamRequest.extract_type: ("_parse_streaming_message",),
    }

    @staticmethod
    def _standardize_columns(
        df: pl.LazyFrame, product: BaseProduct, resolution: Resolution
    ) -> pl.LazyFrame:
        """Standardizes the columns of a DataFrame.
        Adds columns 'product', 'resolution'
        """
        from pfeed.schemas.market_data_schema import PRODUCT, RESOLUTION

        return df.with_columns(
            pl.lit(product.name).alias(PRODUCT),
            # NOTE: str(resolution) is used, NOT repr(resolution)
            pl.lit(str(resolution)).alias(RESOLUTION),
        )

    @staticmethod
    def _filter_columns(df: pl.LazyFrame, product: BaseProduct) -> pl.LazyFrame:
        """Keeps only the standard columns of cleaned data, plus the optional ones present."""
        from pfeed.schemas.bar_data_schema import CLOSE, HIGH, LOW, OPEN
        from pfeed.schemas.market_data_schema import KEY_COLS, VOLUME
        from pfeed.schemas.tick_data_schema import PRICE, SIDE

        cols = df.collect_schema().names()
        is_tick_data = PRICE in cols
        if is_tick_data:
            value_cols = [PRICE, VOLUME, SIDE]
            # side is a standard column even if the source has no trade direction, null means unknown
            if SIDE not in cols:
                df = df.with_columns(pl.lit(None, dtype=pl.Int8).alias(SIDE))
        else:
            value_cols = [OPEN, HIGH, LOW, CLOSE, VOLUME]
        standard_cols = [*KEY_COLS, *value_cols]

        product_specific_cols = []
        if product.is_stock() or product.is_etf():
            from pfeed.schemas.stock_data_schema import DIVIDENDS, SPLITS

            product_specific_cols.extend([DIVIDENDS, SPLITS])

        for col in product_specific_cols:
            if col in cols:
                standard_cols.append(col)
        return df.select(standard_cols)

    @staticmethod
    def _organize_columns(df: pl.LazyFrame) -> pl.LazyFrame:
        """Moves the key columns ('date', 'product', 'resolution') to the leftmost side."""
        from pfeed.schemas.market_data_schema import KEY_COLS

        left_cols = list(KEY_COLS)
        current_cols = df.collect_schema().names()
        target_cols = left_cols + [c for c in current_cols if c not in left_cols]
        if current_cols == target_cols:
            return df
        return df.select(target_cols)

    @staticmethod
    def _resample_data(
        df: pl.LazyFrame,
        product: BaseProduct,
        resolution: str | Resolution,
        offset: str | None = None,
    ) -> pl.LazyFrame:
        """Resamples the input lazyframe based on the target resolution.

        Args:
            df: The input lazyframe to be resampled.
            product: Product instance of the data, e.g. created from `'BTC_USDT_PERP'`.
                Decides the product-specific columns to keep and aggregate,
                e.g. `dividends` and `splits` for stocks and ETFs.
            resolution: The target resolution to resample the data to.
            offset: Optional polars duration string (e.g., '30m') to shift the resampling window.
                For example, with '1h' resolution and '30m' offset, timestamps will be XX:30:00.

        Returns:
            The resampled lazyframe.
        """
        if isinstance(resolution, str):
            resolution = Resolution(resolution)

        from pfeed.schemas.bar_data_schema import CLOSE, HIGH, LOW, OPEN
        from pfeed.schemas.market_data_schema import PRODUCT, RESOLUTION, VOLUME
        from pfeed.schemas.tick_data_schema import PRICE
        from pfeed.schemas.time_based_data_schema import DATE

        df_resolution_str = df.select(pl.col(RESOLUTION).first()).collect().item()
        df_resolution = Resolution(df_resolution_str)
        is_resample_required = resolution < df_resolution
        if not is_resample_required:
            return df

        df = MarketFeed._filter_columns(df, product)

        # pfund's timeframe units (s, m, h, d, w, mo, y) are the same as polars' duration units
        eresolution = f"{resolution.period}{resolution.timeframe.canonical}"

        cols = df.collect_schema().names()
        is_tick_data = PRICE in cols

        aggs: list[pl.Expr] = []
        if is_tick_data:
            aggs.extend(
                [
                    pl.col(PRICE).first().alias(OPEN),
                    pl.col(PRICE).max().alias(HIGH),
                    pl.col(PRICE).min().alias(LOW),
                    pl.col(PRICE).last().alias(CLOSE),
                    pl.col(VOLUME).sum().alias(VOLUME),
                ]
            )
        else:
            aggs.extend(
                [
                    pl.col(OPEN).first().alias(OPEN),
                    pl.col(HIGH).max().alias(HIGH),
                    pl.col(LOW).min().alias(LOW),
                    pl.col(CLOSE).last().alias(CLOSE),
                    pl.col(VOLUME).sum().alias(VOLUME),
                ]
            )
        aggs.append(pl.col(PRODUCT).first().alias(PRODUCT))

        if product.is_stock() or product.is_etf():
            from pfeed.schemas.stock_data_schema import DIVIDENDS, SPLITS

            if DIVIDENDS in cols:
                aggs.append(pl.col(DIVIDENDS).sum().alias(DIVIDENDS))
            if SPLITS in cols:
                aggs.append(pl.col(SPLITS).product().alias(SPLITS))

        aggs.append(pl.len().alias("n_data_points"))

        resampled = (
            df.sort(DATE)
            .group_by_dynamic(
                DATE,
                every=eresolution,
                offset=offset,
                label="left",
                closed="left",
            )
            .agg(aggs)
            .with_columns(pl.lit(str(resolution)).alias(RESOLUTION))
        )
        return resampled

    def get_supported_resolutions(
        self, include_resampled: bool = False
    ) -> list[Resolution]:
        """Get all supported resolutions for batch processing for the data source.

        Args:
            include_resampled: If False (default), return only the resolutions the
                data source literally provides. If True, also include all coarser
                resolutions derivable by resampling from the finest native one —
                e.g. a source providing '1t' also "supports" '1s', '1m', '1h', '1d', etc.
                Quote resolutions pass through but are never a resampling base, so a
                quote-only source returns only its native resolutions.
        """
        native = [
            Resolution(dtype_or_resol)
            for dtype_or_resol in self.data_source.data_categories[self.data_domain]
        ]
        if not include_resampled or not native:
            return native
        resample_bases = [r for r in native if not r.is_quote()]
        if not resample_bases:
            return native
        finest = max(resample_bases)
        return sorted(
            set(native) | set(finest.get_coarser_resolutions(exclude_quote=True)),
            reverse=True,
        )

    @classmethod
    def create_data_model(
        cls,
        product: BaseProduct | str,
        resolution: Resolution | MarketDataType | str,
        start_date: datetime.date | str,
        end_date: datetime.date | str | None = None,
        env: Environment | str = Environment.BACKTEST,
        data_origin: str = "",
        **product_specs: Any,
    ) -> MarketDataModel:
        """Create a MarketDataModel instance.

        Args:
            product: Product basis, e.g. `'BTC_USDT_PERP'`, or a product instance.
            resolution: Resolution, e.g. `'1t'`, `'1m'`, a `MarketDataType` e.g. "minute", or a `Resolution` instance.
            start_date: First date (inclusive), e.g. `'2025-01-01'`, or a `datetime.date`.
            end_date: Last date (inclusive). If None, defaults to `start_date` (a single-day model).
            env: Trading environment.
            data_origin: Sub-label for data from the same source but different origins.
                Defaults to the source name.
            product_specs: Extra product attributes, e.g. `expiration='2025-12-26'` for
                futures. Leave them out to get an error listing the required ones.
        """
        if isinstance(product, str):
            product = cls.DataSource.create_product(product, **product_specs)
        return cls.DataModel(
            env=env,
            data_source=cls.DataSource.metadata.name,
            data_origin=data_origin,
            product=product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date or start_date,
        )

    # NOTE: the return type follows the pipeline mode, see BaseFeed's PipelineT
    @overload
    def download[F: MarketFeed[Any, Any, Any, Literal[True]]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> F: ...
    @overload
    def download(
        self: MarketFeed[Any, Any, Any, Literal[False]],
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> RunResult: ...
    @overload
    def download[F: MarketFeed[Any, Any, Any, bool]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> F | RunResult: ...
    @requires("download")
    def download(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Download historical data from the data source.

        Args:
            product: Product basis, e.g. `'BTC_USDT_PERP'`, `'AAPL_USD_STK'`.
            resolution: Target resolution, e.g. `'1t'` (tick), `'1m'`, `'1d'`.
                If the source doesn't provide it, the closest finer resolution is
                downloaded and downsampled.
            rollback_period: How far to roll back from today (UTC) when `start_date`
                is None, e.g. `'7d'` = the last 7 days up to yesterday.
                Also accepts `'ytd'` (year to date) and `'max'` (all available data).
            start_date: First date (inclusive), e.g. `'2025-01-01'`.
            end_date: Last date (inclusive). If None, defaults to yesterday (UTC).
                Requires `start_date`.
            data_origin: Sub-label for data from the same source but different origins.
                Defaults to the source name.
            data_layer: Data layer of the downloaded data, `'raw'` or `'cleaned'` (default).
                `'cleaned'` runs the default transformations (normalize, standardize columns,
                downsample); `'raw'` returns the data as-is. The data is stored in this layer if `io` is given.
            io: Where and how to store the data. If None, data is not stored.
                e.g. `ParquetIO(base_path='./data')`, `DuckLakeIO()`
            product_specs: Extra product attributes, e.g. `expiration='2025-12-26'` for
                futures. Leave them out to get an error listing the required ones.

        Returns:
            `RunResult` with the downloaded data, or `self` in pipeline mode.
        """
        env = Environment.BACKTEST
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, **product_specs)
        resolution = Resolution(resolution)
        start_date, end_date = self._standardize_dates(
            resolution, start_date, end_date, rollback_period
        )
        if not (
            candidates := [
                r for r in self.get_supported_resolutions() if r >= resolution
            ]
        ):
            raise ValueError(f"{resolution} is not supported by {self.name}")
        # find the first resolution that is >= (finer/higher) the target resolution
        data_resolution = min(candidates)
        request = cast(
            "RequestT",
            cast("type[MarketFeedDownloadRequest]", self.Requests.download)(
                data_source=self.data_source,
                data_origin=data_origin,
                io=io,
                data_layer=data_layer,
                env=env,
                product=product,
                target_resolution=resolution,
                data_resolution=data_resolution,
                start_date=start_date,
                end_date=end_date,
            ),
        )
        self._append_request(request)
        data_model = cast("DataModelT", request.to_data_model())
        self._create_batch_dataflows(
            extract_func=self._download_impl,
            data_models=data_model.split_by_date()
            if self.download_dataflow_per_date
            else [data_model],
        )
        return self.run() if not self.is_pipeline() else self

    @staticmethod
    def _normalize_downloaded_data(df: pl.LazyFrame) -> pl.LazyFrame:
        raise NotImplementedError

    @staticmethod
    def _download_impl(
        request: RequestT, data_model: DataModelT
    ) -> pl.LazyFrame | None:
        raise NotImplementedError

    def _get_default_transformations(
        self, request: RequestT, /
    ) -> list[Callable[..., Any]]:
        match request.extract_type:
            case "download":
                return self._get_default_transformations_for_download(request)
            case "retrieve":
                return self._get_default_transformations_for_retrieve(request)
            case "stream":
                return self._get_default_transformations_for_stream(request)
            case "replay":
                return self._get_default_transformations_for_replay(request)
            case _:
                raise ValueError(f"Unknown extract type: {request.extract_type}")

    def _get_default_transformations_for_download(
        self, request: RequestT, /
    ) -> list[Callable[..., Any]]:
        from pfeed.utils import lambda_with_name

        # bind the class and plain values so the lambdas don't capture self (the whole feed) when pickled by Ray
        cls = type(self)
        is_raw_data = not request.should_clean_data()
        product, data_resolution, target_resolution = (
            request.product,
            request.data_resolution,
            request.target_resolution,
        )
        default_transformations = [
            lambda_with_name(
                "standardize_date_column",
                lambda df: cls._standardize_date_column(df, is_raw_data=is_raw_data),
            ),
        ]
        if is_raw_data:
            return default_transformations
        default_transformations.extend(
            [
                cls._normalize_downloaded_data,
                lambda_with_name(
                    "standardize_columns",
                    lambda df: cls._standardize_columns(df, product, data_resolution),
                ),
                lambda_with_name(
                    "resample_data_if_necessary",
                    lambda df: cls._resample_data(df, product, target_resolution),
                ),
                cls._organize_columns,
            ]
        )
        return default_transformations

    # NOTE: the return type follows the pipeline mode, see BaseFeed's PipelineT
    @overload
    def retrieve[F: MarketFeed[Any, Any, Any, Literal[True]]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        env: Environment | str = Environment.BACKTEST,
        dataflow_per_date: bool = False,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> F: ...
    @overload
    def retrieve(
        self: MarketFeed[Any, Any, Any, Literal[False]],
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        env: Environment | str = Environment.BACKTEST,
        dataflow_per_date: bool = False,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> RunResult: ...
    @overload
    def retrieve[F: MarketFeed[Any, Any, Any, bool]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        env: Environment | str = Environment.BACKTEST,
        dataflow_per_date: bool = False,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> F | RunResult: ...
    @requires("retrieve")
    def retrieve(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        env: Environment | str = Environment.BACKTEST,
        dataflow_per_date: bool = False,
        io: BaseIO | None = None,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Retrieve data from storage.

        Args:
            product: Product basis, e.g. `'BTC_USDT_PERP'`, `'AAPL_USD_STK'`.
            resolution: Target resolution, e.g. `'1t'` (tick), `'1m'`, `'1d'`.
                If not stored, finer stored data is retrieved and downsampled.
            rollback_period: How far to roll back from today (UTC) when `start_date`
                is None, e.g. `'7d'` = the last 7 days up to yesterday.
                Also accepts `'ytd'` (year to date) and `'max'` (all available data).
            start_date: First date (inclusive), e.g. `'2025-01-01'`.
            end_date: Last date (inclusive). If None, defaults to yesterday (UTC).
                Requires `start_date`.
            data_origin: Sub-label for data from the same source but different origins.
                Defaults to the source name.
            data_layer: Data layer to retrieve the data from:
                `'raw'`, `'cleaned'` (default) or `'curated'`. Data is returned as stored, raw data is not cleaned.
            env: Trading environment the data was stored in: `'BACKTEST'` (default) for
                downloaded data, `'PAPER'` or `'LIVE'` for streamed data.
            dataflow_per_date: Whether to create one dataflow per date.
                If False (default), all dates are retrieved in one dataflow
                (one multi-file scan), fastest for typical queries.
                Set True when:

                - all dates don't fit in memory at once, e.g. when downsampling
                - using Ray, to parallelize per-date tasks across workers
            io: Where and how to retrieve the data from. If None, uses `ParquetIO()`.
                e.g. `ParquetIO(base_path='./data')`, `DuckLakeIO()`
            product_specs: Extra product attributes, e.g. `expiration='2025-12-26'` for
                futures. Leave them out to get an error listing the required ones.

        Returns:
            `RunResult` with the retrieved data, or `self` in pipeline mode.
        """
        from functools import partial

        from pfeed.io.parquet_io import ParquetIO

        env = Environment(env.upper())
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, **product_specs)
        resolution = Resolution(resolution)
        start_date, end_date = self._standardize_dates(
            resolution, start_date, end_date, rollback_period
        )
        data_layer = DataLayer.parse(data_layer)
        # search for higher resolutions (closest first), e.g. if resolution is '1m', search '1m' -> '1s' -> '1t'
        # only cleaned data has a known schema to resample, raw and curated data only use the requested resolution
        search_resolutions = (
            [resolution]
            if data_layer != DataLayer.CLEANED
            else [
                resolution,
                *sorted(
                    [
                        _resolution
                        for _resolution in self.get_supported_resolutions(
                            include_resampled=True
                        )
                        # quote data has no price or OHLC columns to resample into bars
                        if _resolution > resolution and not _resolution.is_quote()
                    ]
                ),
            ]
        )

        # find the data resolution: the first search resolution stored for every date,
        # falls back to the target resolution if none is fully stored
        partial_request = partial(
            cast("type[MarketFeedRetrieveRequest]", self.Requests.retrieve),
            data_source=self.data_source,
            data_origin=data_origin,
            io_for_retrieval=io or ParquetIO(),
            data_layer=data_layer,
            env=env,
            product=product,
            target_resolution=resolution,
            start_date=start_date,
            end_date=end_date,
        )
        # data probing, checks storage before any dataflow is created, so the request carries the exact data_resolution to read
        missing_dates: list[datetime.date] = []
        for search_resolution in search_resolutions:
            request = partial_request(data_resolution=search_resolution)
            if not (
                _missing_dates
                := request.create_data_resolution_handler().find_missing_dates_in_storage()
            ):
                break
            # the target resolution is the one fallen back to, report its missing dates
            if search_resolution == resolution:
                missing_dates = _missing_dates
        else:
            # list at most 10 missing dates, a long range can miss thousands
            shown_dates = ", ".join(str(date) for date in missing_dates[:10]) + (
                ", ..." if len(missing_dates) > 10 else ""
            )
            self.logger.warning(
                f"{product} data from {start_date} to {end_date} is not fully stored at any of {search_resolutions} in {request.io_for_retrieval!r}, "
                + f"falling back to {resolution}, missing {len(missing_dates)} dates: {shown_dates}"
            )
            request = partial_request(data_resolution=resolution)
        self._append_request(request)
        data_model = cast("DataModelT", request.to_data_model())
        self._create_batch_dataflows(
            extract_func=self._retrieve_impl,
            data_models=data_model.split_by_date()
            if dataflow_per_date
            else [data_model],
        )
        return self.run() if not self.is_pipeline() else self

    @staticmethod
    def _retrieve_impl(
        request: RequestT, data_model: DataModelT
    ) -> pl.LazyFrame | None:
        # data_model is of target resolution and has the dataflow's date range (one date when dataflow_per_date=True),
        # the handler reads that range at data_resolution
        assert isinstance(request, MarketFeedRetrieveRequest)
        lf, _ = request.create_data_resolution_handler(data_model).read()
        return lf

    def _get_default_transformations_for_retrieve(
        self, request: RequestT, /
    ) -> list[Callable[..., Any]]:
        from pfeed.utils import lambda_with_name

        default_transformations = []

        if cast("MarketFeedRetrieveRequest", request).is_streamed_data():
            import polars.selectors as cs

            from pfeed.schemas.market_data_schema import RESOLUTION
            from pfeed.schemas.streaming.bar_message_schema import START_TS
            from pfeed.schemas.streaming.market_message_schema import RECEIVED_AT
            from pfeed.schemas.streaming.tick_message_schema import TS
            from pfeed.schemas.time_based_data_schema import DATE

            # a bar's date is its start, like in batch data, see MarketDataHandler._get_date_col()
            date_col = START_TS if request.data_resolution.is_bar() else TS
            data_resolution = request.data_resolution
            default_transformations.append(
                lambda_with_name(
                    "streaming_to_batch_schema",
                    # stored as UTC, returned tz-naive UTC like batch data, e.g. start_ts, received_at;
                    # streamed data has no resolution column;
                    # sorted like batch data, the table IO doesn't keep the stored order, e.g. DuckLake's inlined rows and files,
                    # received_at orders the updates of the same bar, e.g. incremental bars
                    lambda df: (
                        df.with_columns(
                            cs.datetime(time_zone="*").dt.replace_time_zone(None),
                        )
                        .with_columns(
                            pl.col(date_col).alias(DATE),
                            pl.lit(str(data_resolution)).alias(RESOLUTION),
                        )
                        .sort(date_col, RECEIVED_AT)
                    ),
                ),
            )
        # only cleaned data has a known schema to resample and organize, raw and curated data are returned as stored
        if request.data_layer == DataLayer.CLEANED:
            # bind the class and plain values so the lambdas don't capture self (the whole feed) when pickled by Ray
            cls = type(self)
            product, target_resolution = request.product, request.target_resolution
            default_transformations.extend(
                [
                    lambda_with_name(
                        "resample_data_if_necessary",
                        lambda df: cls._resample_data(df, product, target_resolution),
                    ),
                    cls._organize_columns,
                ]
            )
        return default_transformations

    # NOTE: the return type follows the pipeline mode, see BaseFeed's PipelineT
    @overload
    def stream[F: MarketFeed[Any, Any, Any, Literal[True]]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        data_origin: str = "",
        io: BaseIO | None = None,
        env: Literal[
            Environment.PAPER, Environment.LIVE, "PAPER", "LIVE"
        ] = Environment.LIVE,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        store_incremental_bars: bool = False,
        flush_interval: float = 100,
        **product_specs: Any,
    ) -> F: ...
    @overload
    def stream(
        self: MarketFeed[Any, Any, Any, Literal[False]],
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        data_origin: str = "",
        io: BaseIO | None = None,
        env: Literal[
            Environment.PAPER, Environment.LIVE, "PAPER", "LIVE"
        ] = Environment.LIVE,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        store_incremental_bars: bool = False,
        flush_interval: float = 100,
        **product_specs: Any,
    ) -> RunResult: ...
    @overload
    def stream[F: MarketFeed[Any, Any, Any, bool]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        data_origin: str = "",
        io: BaseIO | None = None,
        env: Literal[
            Environment.PAPER, Environment.LIVE, "PAPER", "LIVE"
        ] = Environment.LIVE,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        store_incremental_bars: bool = False,
        flush_interval: float = 100,
        **product_specs: Any,
    ) -> F | RunResult: ...
    @requires("stream")
    def stream(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        data_origin: str = "",
        io: BaseIO | None = None,
        env: Literal[
            Environment.PAPER, Environment.LIVE, "PAPER", "LIVE"
        ] = Environment.LIVE,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        store_incremental_bars: bool = False,
        flush_interval: float = 100,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Stream data live from the data source, to replay stored data use replay().

        Args:
            product: Product basis, e.g. `'BTC_USDT_PERP'`, `'AAPL_USD_STK'`.
            resolution: Target resolution, e.g. `'1t'` (tick), `'1m'`, `'1d'`.
                If the source doesn't stream it, a finer streamed resolution is used and resampled.
            data_origin: Sub-label for data from the same source but different origins.
                Defaults to the source name.
            io: Where to store the streamed data. If None, data is not stored.
                e.g. `ParquetIO(base_path='./data')`, `DuckLakeIO()`
            env: Trading environment, `'PAPER'` or `'LIVE'` (default).
            callback: Sync or async callable called with each streamed `Message`, before it is transformed or stored.
                `Message['data']` holds the parsed data as a list, e.g. bars or ticks,
                except when using Ray (see `set_num_workers()`), where the workers parse it and `Message['data']` is None.
                It is awaited before the next message is read, so a slow callback delays every stream of the feed,
                offload heavy work, e.g. to a task or a thread.
            store_incremental_bars: Whether to also store the updates of a bar before it closes,
                e.g. for a venue that only streams such updates. If False (default), only closed bars are stored.
                Only meaningful for bar resolutions when `io` is given.
            flush_interval: Seconds between writes of the buffered streamed data to `io`.
                Frequent writes create many small files, which slow down reads until the table is compacted.
                Only meaningful when `io` is given.
            product_specs: Extra product attributes, e.g. `expiration='2025-12-26'` for
                futures. Leave them out to get an error listing the required ones.

        Returns:
            `RunResult` once the streams end, or `self` in pipeline mode.
        """
        from pfund_kit.utils.temporal import get_utc_now

        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, **product_specs)
        resolution = Resolution(resolution)
        start_date = end_date = get_utc_now().date()

        data_resolution = resolution
        if resolution.is_bar():
            supported = self.data_source.get_stream_api(env).get_supported_resolutions(
                product
            )
            if resolution not in supported:
                from pfund_blocks.datas.bar_data import BarData

                target = BarData(product, resolution)
                candidates = [r for r in supported if target.can_resample_from(r)]
                if not candidates:
                    raise ValueError(
                        f"{resolution!r} is not supported in streaming by {self.name}"
                    )
                # the coarsest one that fits, e.g. 6m with [1m, 3m, 5m, 1t] -> 3m
                data_resolution = min(candidates)
                self.logger.warning(
                    f"{product} {resolution} is not supported in streaming, using {data_resolution} instead to resample data",
                )

        request = cast(
            "RequestT",
            cast("type[MarketFeedStreamRequest]", self.Requests.stream)(
                data_source=self.data_source,
                data_origin=data_origin,
                io=io,
                data_layer=DataLayer.CLEANED,
                env=env,
                product=product,
                target_resolution=resolution,
                data_resolution=data_resolution,
                start_date=start_date,
                end_date=end_date,
                store_incremental_bars=store_incremental_bars,
                flush_interval=flush_interval,
            ),
        )
        self._append_request(request)
        data_model = cast("DataModelT", request.to_data_model())
        self._create_stream_dataflow(
            extract_func=self._stream_impl,
            data_model=data_model,
            callback=callback,
        )
        return self.run() if not self.is_pipeline() else self

    @staticmethod
    def _create_queue_key_from_request(request: RequestT) -> tuple[ProductName, str]:
        return (request.product.name, repr(request.data_resolution))

    @staticmethod
    def _create_queue_key_from_message(message: Message) -> Hashable:
        return (message["product"], message["resolution"])

    def _get_handler_kwargs(self, request: RequestT) -> dict[str, Any]:
        if request.extract_type == "stream":
            return {
                "store_incremental_bars": cast(
                    "MarketFeedStreamRequest", request
                ).store_incremental_bars
            }
        return {}

    @staticmethod
    def _parse_streaming_message(msg: Message) -> Message:
        raise NotImplementedError

    async def _stream_impl(self, request: RequestT):
        """Streams from the data source's stream API (env=PAPER/LIVE), replaying (env=BACKTEST) is _replay_impl()."""
        stream_api = self.data_source.get_stream_api(
            cast("Literal[Environment.PAPER, Environment.LIVE]", request.env)
        )
        try:
            await stream_api.connect()
            # NOTE: connect() returns right away, this is what notices that the stream has stopped
            await stream_api.wait_until_disconnected()
        finally:
            try:
                # NOTE: cancelling wait_until_disconnected() doesn't disconnect the websockets
                await stream_api.disconnect()
            finally:
                for queue in self._queues.values():
                    queue.shutdown()

    # NOTE: ALL transformation functions MUST be static methods so that they can be serialized by Ray
    def _get_default_transformations_for_stream(
        self, request: RequestT
    ) -> list[Callable[..., Any]]:
        from pfund_blocks.datas.bar_data import BarData
        from pfund_blocks.datas.tick_data import TickData

        from pfeed.utils import lambda_with_name

        default_transformations: list[Callable[..., Any]] = [
            self._parse_streaming_message
        ]

        is_resampling = bool(request.target_resolution < request.data_resolution)
        if is_resampling:
            self.logger.warning(
                f"{request.product.name} {request.target_resolution!r} bars will be created by resampling {request.data_resolution!r} data, "
                + "incremental bars (not closed yet) will also be sent, check bar.is_closed if you only want closed bars"
            )
            product = request.product
            target_resolution = request.target_resolution
            data_resolution = request.data_resolution
            # use data_bar to resample data, e.g. bybit doesn't support '1s' data (target resolution), use '1t' (data resolution) instead
            data_bar = BarData(product=product, resolution=target_resolution)
            # the resampler takes every update in data resolution, and only passes what can be merged to data_bar (its resamplee),
            # i.e. every tick, but only closed bars, an unclosed bar's volume would otherwise be counted at each of its updates
            if data_resolution.is_tick():
                resampler = TickData(product=product, resolution=data_resolution)
            elif data_resolution.is_bar():
                resampler = BarData(product=product, resolution=data_resolution)
            else:
                raise TypeError(f"Invalid {data_resolution=} for resampling")
            # raises if the resampler's bars don't fit in data_bar's, e.g. 3m bars can't be resampled to 5m bars
            data_bar.bind_resampler(resampler)
            # bind the class and plain values so the lambda doesn't capture self (the whole feed) when pickled by Ray
            cls = type(self)
            default_transformations.append(
                lambda_with_name(
                    "resample_streaming_message",
                    lambda msg: cls._resample_streaming_message(
                        message=msg,
                        resampler=resampler,
                        data_bar=data_bar,
                        target_resolution=target_resolution,
                    ),
                )
            )
        return default_transformations

    @staticmethod
    def _resample_streaming_message(
        message: Message,
        resampler: MarketData,
        data_bar: BarData,
        target_resolution: Resolution,
    ) -> Message:
        """Updates the resampler with the message's data points, returns the message with data_bar's bars as its data.

        The data is every bar closed by the update in time order, or data_bar's open bar if it changed but nothing closed.
        It is None if the update didn't change data_bar, e.g. an unclosed finer bar, which the resampler keeps until it closes.
        """
        current_bar = data_bar.bar
        # NOTE: a parsed message can hold several data points (e.g. Bybit sends a list), each is updated in order
        data_points = cast("list[Bar | Tick]", message["data"])
        # finest first, so the resampler's own closed bars come before data_bar's
        closed_bars = [
            bar
            for data_point in data_points
            for bar in resampler.update(data_point)
            if bar.resolution == target_resolution
        ]
        # case 1: the update closed bars, e.g. the 1m bar ending at 9:05 closes the 9:00 5m bar,
        # they are sent with their final states, the bar opened by the same update (if any) is sent with the next update
        # NOTE: a message with several data points can close several bars, e.g. a batch of trades spanning seconds
        if closed_bars:
            resampled_bars = closed_bars
        # case 2: the update changed the open bar, e.g. the 9:01 1m bar closes and is merged into the open 9:00 5m bar
        # Bar is immutable, so a changed data_bar has a new bar object
        elif data_bar.bar is not current_bar:
            resampled_bars = [data_bar.bar]
        # case 3: nothing changed, e.g. an unclosed 1m bar, or no 1m bar has closed since the stream started
        else:
            resampled_bars = None
        return {
            **message,
            "resolution": repr(target_resolution),
            "data": resampled_bars,
        }

    # NOTE: the return type follows the pipeline mode, see BaseFeed's PipelineT
    @overload
    def replay[F: MarketFeed[Any, Any, Any, Literal[True]]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str = "",
        end_date: datetime.date | str = "",
        data_origin: str = "",
        io: BaseIO | None = None,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        replay_pace: float | None = 0,
        **product_specs: Any,
    ) -> F: ...
    @overload
    def replay(
        self: MarketFeed[Any, Any, Any, Literal[False]],
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str = "",
        end_date: datetime.date | str = "",
        data_origin: str = "",
        io: BaseIO | None = None,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        replay_pace: float | None = 0,
        **product_specs: Any,
    ) -> RunResult: ...
    @overload
    def replay[F: MarketFeed[Any, Any, Any, bool]](
        self: F,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str = "",
        end_date: datetime.date | str = "",
        data_origin: str = "",
        io: BaseIO | None = None,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        replay_pace: float | None = 0,
        **product_specs: Any,
    ) -> F | RunResult: ...
    @requires("replay")
    def replay(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str = "",
        end_date: datetime.date | str = "",
        data_origin: str = "",
        io: BaseIO | None = None,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
        replay_pace: float | None = 0,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Replay stored data at the cleaned data layer as a stream, e.g. for backtesting.

        Every replay of the feed is merged into one stream, ordered by when each message would have been received live,
        i.e. a bar at its end, a tick at its timestamp.

        Args:
            product: Product basis, e.g. `'BTC_USDT_PERP'`, `'AAPL_USD_STK'`.
            resolution: Resolution of the stored data to replay, e.g. `'1t'` (tick), `'1m'`, `'1d'`.
            rollback_period: How far to roll back from today (UTC) when `start_date`
                is empty, e.g. `'7d'` = the last 7 days up to yesterday.
                Also accepts `'ytd'` (year to date) and `'max'` (all available data).
            start_date: First date (inclusive), e.g. `'2025-01-01'`.
            end_date: Last date (inclusive). If empty, defaults to yesterday (UTC).
                Requires `start_date`.
            data_origin: Sub-label for data from the same source but different origins.
                Defaults to the source name.
            io: Where to replay the data from. If None, uses `ParquetIO()`.
                e.g. `ParquetIO(base_path='./data')`, `DuckLakeIO()`
            callback: Sync or async callable called with each replayed `Message`, before it is transformed.
                `Message['data']` holds a list with one closed bar or one tick, like a streamed message holds a list of them.
                It is awaited before the next message is replayed, so a slow callback slows down the replay.
            replay_pace: Pacing between messages.
                - 0 (default): ASAP — no sleep between messages. Backtests process the whole
                  range as fast as possible regardless of resolution or row count.
                - >0: fixed cadence in seconds (e.g. 1.0 → one message per wall-second).
                  Useful for watching a replay at a steady, human-readable rate.
                - None: realistic — sleeps the time between consecutive messages, at most one bar period for bars,
                  so data gaps (e.g. weekends) aren't slept through.
                  Opt-in only: for fine resolutions or tick data this can take hours, so it is not the default.
            product_specs: Extra product attributes, e.g. `expiration='2025-12-26'` for
                futures. Leave them out to get an error listing the required ones.

        Returns:
            `RunResult` once the replay ends, or `self` in pipeline mode.
        """
        from pfeed.io.parquet_io import ParquetIO

        env = Environment.BACKTEST
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, **product_specs)
        resolution = Resolution(resolution)
        start_date, end_date = self._standardize_dates(
            resolution, start_date, end_date, rollback_period
        )
        request = cast(
            "RequestT",
            cast("type[MarketFeedReplayRequest]", self.Requests.replay)(
                data_source=self.data_source,
                data_origin=data_origin,
                io_for_replay=io or ParquetIO(),
                data_layer=DataLayer.CLEANED,
                env=env,
                product=product,
                target_resolution=resolution,
                # NOTE: replays the stored data as is, no resampling
                data_resolution=resolution,
                start_date=start_date,
                end_date=end_date,
                replay_pace=replay_pace,
            ),
        )
        self._append_request(request)
        data_model = cast("DataModelT", request.to_data_model())
        self._create_stream_dataflow(
            extract_func=self._replay_impl,
            data_model=data_model,
            callback=callback,
        )
        return self.run() if not self.is_pipeline() else self

    async def _replay_impl(self, request: RequestT):
        """Replays the stored data of every stream request of the feed (env=BACKTEST), merged by the time each message would have been received live.

        The extract function of replay(), like _stream_impl() is for stream().
        """
        import asyncio
        from collections import defaultdict
        from operator import itemgetter

        from pfund_blocks.datas.bar_data import Bar
        from pfund_blocks.datas.tick_data import Tick
        from pfund_blocks.enums.side import Side
        from pfund_blocks.utils.temporal import NS_PER_SECOND

        from pfeed.schemas.bar_data_schema import CLOSE, HIGH, LOW, OPEN
        from pfeed.schemas.market_data_schema import VOLUME
        from pfeed.schemas.tick_data_schema import PRICE, SIDE
        from pfeed.schemas.time_based_data_schema import DATE
        from pfeed.streaming.replay_clock import ReplayClock

        # NOTE: all streams of a feed share one faucet, which runs this once with the first request;
        # like a stream API holding every channel, replay reads every request and merges their data by time
        requests = [cast("MarketFeedReplayRequest", req) for req in self._requests]
        # NOTE: with the data engine, its clock merges the replays of all its feeds by time, it has joined this feed already
        clock = self._engine._replay_clock if self._engine else None
        if clock is None:
            clock = ReplayClock(cast("MarketFeedReplayRequest", request).replay_pace)
            clock.join(self)

        def _read_day(
            handlers: list[tuple[MarketFeedReplayRequest, MarketDataHandler]],
        ) -> list[tuple[int, Message]]:
            """Returns the day's messages of every request, each with the time it would have been received live, ordered by it."""
            messages: list[tuple[int, Message]] = []
            for req, handler in handlers:
                lf, _ = handler.read()
                if lf is None:
                    continue
                # NOTE: as int ns, iter_rows() would return a python datetime, which only has microseconds
                df = lf.with_columns(pl.col(DATE).cast(pl.Int64)).collect()
                resolution = req.data_resolution
                for row in df.iter_rows(named=True):
                    data: Bar | Tick
                    if resolution.is_bar():
                        # NOTE: a bar's date is its start, but it is only known once closed, so it is received at its end.
                        # columns a Bar doesn't have are not replayed, e.g. a stock's dividends and splits
                        start_ts: int = row[DATE]
                        end_ts = start_ts + resolution.to_seconds() * NS_PER_SECOND
                        data = Bar(
                            resolution=resolution,
                            start_ts=start_ts,
                            end_ts=end_ts,
                            updated_ts=end_ts,
                            open=row[OPEN],
                            high=row[HIGH],
                            low=row[LOW],
                            close=row[CLOSE],
                            volume=row[VOLUME],
                            is_closed=True,
                        )
                        received_at = end_ts
                    else:
                        side = row.get(SIDE)
                        data = Tick(
                            ts=row[DATE],
                            price=row[PRICE],
                            volume=row[VOLUME],
                            side=Side(side) if side is not None else None,
                        )
                        received_at = data.ts
                    message: Message = {
                        "source": req.data_source.name,
                        "account": None,
                        "channel": None,
                        "product": req.product.name,
                        # NOTE: must match the queue key, see _create_queue_key_from_request()
                        "resolution": repr(resolution),
                        "received_at": received_at,
                        "message": None,  # raw message as None
                        # NOTE: a list, like a streamed message, which can hold several data points
                        "data": [data],
                        "error": None,
                        "extra": {},
                    }
                    messages.append((received_at, message))
            # NOTE: a stable sort, messages received at the same time stay in request order, then in stored order
            messages.sort(key=itemgetter(0))
            return messages

        try:
            handlers_per_date: dict[
                datetime.date, list[tuple[MarketFeedReplayRequest, MarketDataHandler]]
            ] = defaultdict(list)
            for req in requests:
                if not (req.data_resolution.is_bar() or req.data_resolution.is_tick()):
                    raise NotImplementedError(
                        f"replaying {req.data_resolution!r} data is not supported yet"
                    )
                replay_data_model = req.to_data_model()
                if missing_dates := req.create_data_resolution_handler(
                    replay_data_model
                ).find_missing_dates_in_storage():
                    self.logger.warning(
                        f"{req.product} {req.data_resolution!r} data from {replay_data_model.start_date} to {replay_data_model.end_date} "
                        + f"is missing {len(missing_dates)} dates in {req.io_for_replay!r}, they are skipped in the replay"
                    )
                for daily_data_model in replay_data_model.split_by_date():
                    handlers_per_date[daily_data_model.date].append(
                        (req, req.create_data_resolution_handler(daily_data_model))
                    )
            # NOTE: one day at a time to bound memory
            for date in sorted(handlers_per_date):
                # NOTE: reading blocks, so it runs in a thread, the dataflows keep consuming their queues meanwhile
                messages = await asyncio.to_thread(_read_day, handlers_per_date[date])
                if not messages:
                    self.logger.debug(f"No data to replay on {date}")
                    continue
                for received_at, message in messages:
                    data = cast("list[Bar | Tick]", message["data"])[0]
                    # NOTE: at most one bar period for the realistic pace, so data gaps (e.g. weekends) aren't slept through
                    max_delay = (
                        data.resolution.to_seconds() if isinstance(data, Bar) else None
                    )
                    await clock.wait_until(self, received_at, max_delay=max_delay)
                    await self._callback(message)
        finally:
            clock.leave(self)
            # NOTE: the replay has ended (or failed / was cancelled), ends the dataflows reading the queues
            for queue in self._queues.values():
                queue.shutdown()

    def _get_default_transformations_for_replay(
        self, request: RequestT, /
    ) -> list[Callable[..., Any]]:
        # NOTE: the replayed data is already cleaned
        return []
