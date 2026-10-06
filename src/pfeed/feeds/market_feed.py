from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, cast

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine, Iterator

    from pfund.datas.data_bar import BarData
    from pfund.entities.products.product_base import BaseProduct
    from pfund.venues._apis.typing import ResponseData

    from pfeed.data_handlers.base_data_handler import BaseDataHandler
    from pfeed.dataflow.result import RunResult
    from pfeed.feeds.streaming_feed_mixin import (
        ChannelKey,
        RawMessage,
        ReplayData,
        WebSocketName,
    )
    from pfeed.requests import (
        MarketFeedDownloadRequest,
        MarketFeedRetrieveRequest,
        MarketFeedStreamRequest,
    )
    from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
    from pfeed.source import BaseSource
    from pfeed.streaming.market_data_message import MarketDataMessage

import datetime
import time
from abc import ABC, abstractmethod

import polars as pl
from pfund.datas.resolution import Resolution
from pfund.enums.env import Environment

from pfeed.config import setup_logging
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.enums import DataCategory, DataLayer, MarketDataType
from pfeed.feeds.time_based_feed import TimeBasedFeed
from pfeed.io.base_io import BaseIO
from pfeed.utils.temporal import ns_to_seconds, seconds_to_ns


class MarketFeed(TimeBasedFeed, ABC):
    class Capability(TimeBasedFeed.Capability):
        """Core verbs MarketFeed gates on. Plugins may declare extra verbs as plain strings."""

        download = "download"
        stream = "stream"

    DataModel: ClassVar[type[MarketDataModel]] = MarketDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.MARKET_DATA
    data_source: BaseSource
    SUPPORTS_ROLLBACK_MAX_PERIOD: ClassVar[bool] = False

    @staticmethod
    @abstractmethod
    def _normalize_raw_data(df: pl.LazyFrame) -> pl.LazyFrame:
        pass

    @abstractmethod
    def _download_impl(
        self, data_model: MarketDataModel, data_resolution: Resolution
    ) -> pl.LazyFrame | None:
        pass

    @staticmethod
    def _parse_message(product: BaseProduct, msg: Any) -> ResponseData:
        raise NotImplementedError

    @staticmethod
    def _normalize_timestamps(msg: ResponseData) -> ResponseData:
        """Convert source's native time unit to int ns since epoch.
        Touches top-level `ts` and any timestamp fields inside `data`."""
        raise NotImplementedError

    def get_supported_resolutions(
        self, include_resampled: bool = False
    ) -> list[Resolution]:
        """Get all supported resolutions for batch processing for the data source.

        Args:
            include_resampled: If False (default), return only the resolutions the
                data source literally provides. If True, also include all coarser
                resolutions derivable by resampling from the finest native one —
                e.g. a source providing '1t' also "supports" '1s', '1m', '1h', '1d', etc.
        """
        native = [
            Resolution(dtype_or_resol)
            for dtype_or_resol in self.data_source.METADATA.data_categories[
                DataCategory.MARKET_DATA
            ]
        ]
        if not include_resampled or not native:
            return native
        non_quotes = [r for r in native if not r.is_quote()]
        # TODO: quote data is not handled yet
        if not non_quotes:
            return native
        finest = max(non_quotes)
        return sorted(
            set(native) | set(finest.get_lower_resolutions(exclude_quote=True)),
            reverse=True,
        )

    def create_data_model(
        self,
        product: BaseProduct | str,
        resolution: Resolution | str,
        start_date: datetime.date | str,
        end_date: datetime.date | str | None = None,
        env: Environment | str = Environment.BACKTEST,
        data_origin: str = "",
        **product_specs: Any,
    ) -> MarketDataModel:
        """Create a MarketDataModel instance.

        Args:
            product: product basis (e.g. 'BTC_USDT_PERP') or a Product instance.
            resolution: Data resolution string (e.g. '1m', '1h') or a Resolution instance.
            start_date: Start date as a string ('YYYY-MM-DD') or datetime.date.
            end_date: End date as a string ('YYYY-MM-DD') or datetime.date.
                If None, defaults to start_date, creating a single-day model.
            env: Trading environment.
            data_origin: Origin label for the data.
            product_specs: Additional product specifications (e.g. strike_price, expiration for options).
        """
        DataModel = self.DataModel
        return DataModel(
            env=env,
            data_source=self.data_source,
            data_origin=data_origin,
            product=self.data_source.create_product(product, **product_specs)
            if isinstance(product, str)
            else product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date or start_date,
        )

    def _create_data_model_from_request(
        self, request: MarketFeedBaseRequest
    ) -> MarketDataModel:
        return self.create_data_model(
            product=request.product,
            resolution=request.target_resolution,
            start_date=request.start_date,
            end_date=request.end_date,
            env=request.env,
            data_origin=request.data_origin,
        )

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
        from pfeed.requests import MarketFeedDownloadRequest

        env = Environment.BACKTEST
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, **product_specs)
        resolution = Resolution(resolution)
        start_date, end_date = self._standardize_dates(
            resolution, start_date, end_date, rollback_period
        )
        candidates = [r for r in self.get_supported_resolutions() if r >= resolution]
        if not candidates:
            raise ValueError(f"{resolution} is not supported by {self.name}")
        # find the first resolution that is >= the target resolution
        data_resolution = min(candidates)
        request = MarketFeedDownloadRequest(
            data_source=self.name,
            data_origin=data_origin,
            io=io,
            data_layer=data_layer,
            env=env,
            product=product,
            target_resolution=resolution,
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            dataflow_per_date=self.DOWNLOAD_DATAFLOW_PER_DATE,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(
            extract_func=lambda data_model: self._download_impl(
                data_model=data_model,
                data_resolution=data_resolution,
            )
        )
        return self.run() if not self.is_pipeline() else self

    def _get_default_transformations_for_download(
        self, request: MarketFeedDownloadRequest | MarketFeedRetrieveRequest
    ) -> list[Callable[..., Any]]:
        from pfeed._etl import market as etl
        from pfeed.utils import lambda_with_name

        default_transformations = [
            lambda_with_name(
                "standardize_date_column",
                lambda df: self._standardize_date_column(
                    df, is_raw_data=not request.should_clean_data
                ),
            ),
        ]
        if request.should_clean_data:
            default_transformations.extend(
                [
                    self._normalize_raw_data,
                    lambda_with_name(
                        "standardize_columns",
                        lambda df: etl.standardize_columns(
                            df, request.product, request.data_resolution
                        ),
                    ),
                    lambda_with_name(
                        "resample_data_if_necessary",
                        lambda df: etl.resample_data(
                            df, request.target_resolution, request.product
                        ),
                    ),
                    etl.organize_columns,
                ]
            )
        return default_transformations

    def retrieve(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        *,
        symbol: str = "",
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
            symbol: Source-specific symbol, e.g. `'BTCUSDT'` for Bybit's `BTC_USDT_PERP`.
                If empty, derived from `product`. Pass it explicitly if the derived one is wrong.
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
        from pfeed.requests import MarketFeedRetrieveRequest

        env = Environment[env.upper()]
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(
            product, symbol=symbol, **product_specs
        )
        resolution = Resolution(resolution)
        start_date, end_date = self._standardize_dates(
            resolution, start_date, end_date, rollback_period
        )
        # search for higher resolutions (highest first), e.g. if resolution is '1m', search '1m' -> '1t' -> '1s'
        search_resolutions = [
            resolution,
            *sorted(
                [
                    _resolution
                    for _resolution in self.get_supported_resolutions(
                        include_resampled=True
                    )
                    if _resolution > resolution
                ],
                reverse=True,
            ),
        ]

        if io is None:
            from pfeed.io.parquet_io import ParquetIO

            io = ParquetIO()
        self._validate_io(io)

        # find the data resolution: the first search resolution stored for every date
        data_resolution = None
        for search_resolution in search_resolutions:
            data_model = self.create_data_model(
                env=env,
                product=product,
                resolution=search_resolution,
                start_date=start_date,
                end_date=end_date,
                data_origin=data_origin,
            )
            handler = data_model.DataHandler(
                data_model=data_model,
                io=io,
                data_layer=data_layer,
                data_domain=str(self.data_domain),
            )
            if not handler.find_missing_dates_in_storage():
                data_resolution = search_resolution
                break
        else:
            self.logger.debug(
                f"failed to find stored {product} data from {start_date} to {end_date} with search resolutions {search_resolutions} in {io!r}"
            )

        request = MarketFeedRetrieveRequest(
            data_source=self.name,
            data_origin=data_origin,
            io_for_retrieval=io,
            data_layer=data_layer,
            env=env,
            product=product,
            target_resolution=resolution,
            # NOTE: data_resolution could be None if data of target resolution is not found in storage
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            dataflow_per_date=dataflow_per_date,
        )
        self._append_request(request)
        _ = self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(data_model, request),
        )
        return self.run() if not self.is_pipeline() else self

    def _retrieve_impl(
        self, data_model: MarketDataModel, request: MarketFeedRetrieveRequest
    ) -> pl.LazyFrame | None:
        if request.data_resolution is None:
            self.logger.debug(f"no data found for {data_model} in {request.io_for_retrieval!r}")
            return None
        # data_model is of target resolution, copy it since it has the correct start_date and end_date
        # when dataflow_per_date = True, the handler should read the data model with data_resolution
        data_model = data_model.model_copy(update={"resolution": request.data_resolution})
        handler = data_model.DataHandler(
            data_model=data_model,
            io=request.io_for_retrieval,
            data_layer=request.data_layer,
            data_domain=str(self.data_domain),
        )
        lf, _ = handler.read()
        if lf is not None:
            self.logger.debug(f"retrived data {data_model} from {handler!r}")
        else:
            self.logger.debug(f"no data found for {data_model} in {handler!r}")
        return lf

    def _get_default_transformations_for_retrieve(
        self, request: MarketFeedRetrieveRequest
    ) -> list[Callable[..., Any]]:
        from pfeed._etl import market as etl
        from pfeed.utils import lambda_with_name

        is_retrieving_streaming_data = request.env in (
            Environment.PAPER,
            Environment.LIVE,
        )

        default_transformations = []
        if is_retrieving_streaming_data:
            # a bar's date is its start, like in batch data, see MarketDataHandler._get_date_col()
            date_col = "start_ts" if cast(Resolution, request.data_resolution).is_bar() else "ts"
            default_transformations.append(
                lambda_with_name(
                    "streaming_to_batch_schema",
                    # stored as UTC, batch data's date is tz-naive UTC
                    lambda df: df.with_columns(
                        pl.col(date_col).dt.replace_time_zone(None).alias("date")
                    ),
                ),
            )
        if request.data_layer != DataLayer.RAW:
            default_transformations.extend(
                [
                    lambda_with_name(
                        "resample_data_if_necessary",
                        lambda df: etl.resample_data(
                            df, request.target_resolution, request.product
                        ),
                    ),
                    etl.organize_columns,
                ]
            )
        return default_transformations

    def stream(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        symbol: str = "",
        rollback_period: Resolution | str | Literal["ytd", "max"] = "7d",
        start_date: datetime.date | str = "",
        end_date: datetime.date | str = "",
        callback: Callable[[WebSocketName, RawMessage], Awaitable[None] | None]
        | None = None,
        data_origin: str = "",
        data_layer: DataLayer | str = DataLayer.CLEANED,
        env: Environment | str = Environment.LIVE,
        replay_pace: float | None = 0,
        io: BaseIO | None = None,
        store_incremental_bars: bool = False,
        flush_interval: float = 100,
        **product_specs: Any,
    ) -> Self | None:
        """Stream market data, either live from the data source or by replaying historical data from storage at CLEANED data layer.

        Args:
            product: Product basis (e.g. 'BTC_USDT_PERP', 'AAPL_USD_STK'). For products
                with extra attributes (options, futures), pass them via `product_specs`.
            resolution: Target data resolution (e.g. '1m', '1h', '1d'). If the source
                doesn't provide this resolution natively, finer-grained source data is
                downloaded and resampled down.
            symbol: Source-specific symbol. If empty, derived from `product` — but the
                derivation may be wrong, in which case pass it explicitly.
            rollback_period: Lookback from today, only used when `start_date` is empty.
                Accepts a resolution string (e.g. '7d'), 'ytd', or 'max'. With 'max',
                the source's own `start_date` attribute is used.
                Only meaningful when env=BACKTEST (defines the replay range).
            start_date: Start date. If empty, derived from `rollback_period`.
                Only meaningful when env=BACKTEST.
            end_date: End date. If empty, defaults to today.
                Only meaningful when env=BACKTEST.
            callback: Async or sync callable invoked for each incoming message.
                Receives the raw message dict.
            data_origin: Origin label used to distinguish data from different providers
                of the same source.
            data_layer: Data layer of the streamed data, `'raw'` or `'cleaned'` (default).
                `'cleaned'` runs the default transformations (normalize, standardize columns, resample);
                `'raw'` passes the messages through as-is.
                When replaying (env=BACKTEST), the layer to read from; only `'cleaned'` is supported.
            env: Trading environment. LIVE (default) connects to the live data source
                via websocket. BACKTEST replays historical data from storage.
                only supports BACKTEST, PAPER (paper trading) and LIVE
            replay_pace: Pacing between row emissions when replaying (env=BACKTEST). Ignored otherwise.
                - 0 (default): ASAP — no sleep between rows. Backtests process the whole
                  range as fast as possible regardless of resolution or row count.
                - >0: fixed cadence in seconds (e.g. 1.0 → one row per wall-second).
                  Useful for watching a replay at a steady, human-readable rate.
                - None: realistic — for bars, sleep one resolution period between rows;
                  for ticks, sleep the timestamp difference between consecutive rows.
                  Opt-in only: for fine resolutions or tick data a per-row sleep
                  multiplied by row count can take hours, so it is not the default.
            io: Where and how to store the data. Direction depends on env:
                - LIVE: WHERE to persist streamed data (write destination).
                  If None, streamed data will NOT be persisted.
                - BACKTEST: WHERE to read historical data FROM (read source).
                  If None, defaults to `ParquetIO()`.
            store_incremental_bars: Whether to also store the updates of a bar before it closes,
                e.g. for a venue that only streams such updates. If False (default), only closed bars are stored.
                Only meaningful for bar resolutions when `io` is given and env is not BACKTEST.
            flush_interval: Seconds between writes of the buffered streamed data to `io`.
                Frequent writes create many small files, which slow down reads until the table is compacted.
                Only meaningful when `io` is given and env is not BACKTEST.
            product_specs: Extra product attributes for products that need them, e.g.
                `stream(product='BTC_USDT_OPT', strike_price=10000,
                expiration='2024-01-01', option_type='CALL')`. Leave empty first and
                read the exception message to discover required keys.
        """
        from pfund_kit.utils.temporal import get_utc_now

        from pfeed.requests import MarketFeedStreamRequest

        SUPPORTED_ENVS = [Environment.BACKTEST, Environment.PAPER, Environment.LIVE]
        env = Environment[env.upper()]
        if env not in SUPPORTED_ENVS:
            raise ValueError(f"streaming is only supported in envs {SUPPORTED_ENVS}")
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(
            product, symbol=symbol, **product_specs
        )
        resolution = Resolution(resolution)
        if any([start_date, end_date, rollback_period]):
            start_date, end_date = self._standardize_dates(
                resolution, start_date, end_date, rollback_period
            )
        else:
            today = get_utc_now().date()
            start_date = end_date = today

        is_replaying = env == Environment.BACKTEST
        data_config = None
        data_resolution = resolution
        if not is_replaying:
            if resolution.is_bar():
                from pfund.datas.data_config import DataConfig

                # borrow pfund's data config to reuse its auto-resampling logic to find out data resolution
                # e.g. '1s' is not supported by bybit, '1t' will be used instead to resample data
                data_config = DataConfig(
                    data_source=self.data_source.name, data_origin=data_origin
                )
                data_config.data_resolutions = [data_resolution]
                if product.venue is not None:
                    VenueClass = product.venue.venue_class
                    resampled_data_config = data_config.auto_resample(
                        VenueClass.METADATA.get_supported_resolutions(product)
                    )
                    if resampled_data_config.resample != data_config.resample:
                        data_resolution = resampled_data_config.resample[resolution]
                        self.logger.warning(
                            f"{product.desc_str()} {resolution} is not supported in streaming, using {data_resolution} instead to resample data",
                        )
        else:
            # NOTE: in replay mode, io means loading data FROM storage, not TO storage, so it must exist
            if io is None:
                from pfeed.io.parquet_io import ParquetIO

                io = ParquetIO()
            self._validate_io(io)

        request = MarketFeedStreamRequest(
            data_source=self.name,
            data_origin=data_origin,
            data_config=data_config,
            io=io,
            data_layer=data_layer,
            env=env,
            product=product,
            target_resolution=resolution,
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            replay_pace=replay_pace,
            store_incremental_bars=store_incremental_bars,
            flush_interval=flush_interval,
        )
        self._append_request(request)
        self._create_stream_dataflow(user_callback=callback)
        return self.run() if not self.is_pipeline() else self  # pyright: ignore[reportReturnType]

    async def _stream_impl(
        self,
        data_model: MarketDataModel,
        faucet_callback: Callable[
            [WebSocketName | str, RawMessage | ReplayData, ChannelKey | None],
            Coroutine[Any, Any, None],
        ],
        handler: BaseDataHandler | None = None,
        replay_pace: float | None = None,
    ) -> None:
        from pfund.enums.env import Environment

        stream_api = self.data_source.get_stream_api(env=data_model.env)
        is_replaying = data_model.env == Environment.BACKTEST
        if not is_replaying:
            stream_api.set_callback(faucet_callback)
            await stream_api.connect()
        # replaying BACKTEST data:
        else:
            import asyncio

            assert handler is not None, "handler must be provided for replaying"
            data_source = data_model.data_source.name
            channel_key: ChannelKey = cast(
                "ChannelKey", stream_api.add_channel(data_model)
            )
            start_date, end_date = data_model.start_date, data_model.end_date
            resolution = data_model.resolution
            prev_ts: float | None = None
            # the handler's data model covers start_date to end_date; collected one day at a time to bound memory
            lf, _ = handler.read()
            if lf is None:
                self.logger.warning(f"No data to replay from {start_date} to {end_date}")
                return
            for date in pl.date_range(
                start=start_date, end=end_date, interval="1d", eager=True
            ):
                df = lf.filter(pl.col("date").dt.date() == date).sort("date").collect()
                if df.is_empty():
                    self.logger.debug(f"No data to replay on {date}")
                    continue
                for row in df.iter_rows(named=True):
                    current_ts: float = row["date"].timestamp()
                    if replay_pace == 0:  # ASAP
                        delay = 0.0
                    elif replay_pace is not None:  # custom fixed cadence
                        delay = replay_pace if prev_ts is not None else 0.0
                    # realistic mode
                    else:
                        if (
                            resolution.is_tick()
                        ):  # ticks: use timestamp diff between consecutive rows
                            delay = current_ts - prev_ts if prev_ts is not None else 0.0
                        else:  # bars: use resolution period (insulates from data gaps like weekends)
                            delay = (
                                resolution.to_seconds() if prev_ts is not None else 0.0
                            )
                    if delay > 0:
                        await asyncio.sleep(delay)
                    prev_ts = current_ts
                    await faucet_callback(data_source, row, channel_key)

    # NOTE: ALL transformation functions MUST be static methods so that they can be serialized by Ray
    def _get_default_transformations_for_stream(
        self, request: MarketFeedStreamRequest
    ) -> list[Callable[..., Any]]:
        from itertools import count

        from pfund.datas.data_bar import BarData

        from pfeed.utils import lambda_with_name

        default_transformations: list[Callable[..., Any]] = []

        is_replaying = request.env == Environment.BACKTEST
        # NOTE: replaying backtest data must be cleaned beforehand, no default transformations when env is BACKTEST
        if is_replaying:
            return default_transformations

        if request.should_clean_data:
            # Bind concrete subclass's staticmethod into a local — no `self` captured.
            parse_message = type(self)._parse_message
            default_transformations.extend(
                [
                    lambda_with_name(
                        "parse_message", lambda msg: parse_message(request.product, msg)
                    ),
                    lambda_with_name(
                        "normalize_timestamps",
                        lambda msg: self._normalize_timestamps(msg),
                    ),
                ]
            )
            # NOTE: cannot write self.data_source.name inside self.transform(), otherwise, "self" will be serialized by Ray and return an error
            data_source: str = self.data_source.name
            tick_counter = (
                count() if cast(Resolution, request.data_resolution).is_tick() else None
            )
            is_resampling = bool(request.target_resolution < request.data_resolution)  # pyright: ignore[reportOperatorIssue]
            if is_resampling:
                # use data_bar to resample data, e.g. bybit doesn't support '1s' data (target resolution), use '1t' (data resolution) instead
                data_bar = BarData(
                    product=request.product,
                    resolution=request.target_resolution,
                    config=request.data_config,
                )
            else:
                data_bar = None
            default_transformations.append(
                lambda_with_name(
                    "standardize_message",
                    lambda msg: MarketFeed._standardize_message(
                        data_source=data_source,
                        data_origin=request.data_origin,
                        product=request.product,
                        target_resolution=request.target_resolution,
                        data_resolution=request.data_resolution,
                        msg=msg,
                        data_bar=data_bar,
                        tick_counter=tick_counter,
                    ),
                ),
            )
        return default_transformations

    @staticmethod
    def _standardize_message(
        data_source: str,
        data_origin: str,
        product: BaseProduct,
        target_resolution: Resolution,
        data_resolution: Resolution,
        msg: ResponseData,
        data_bar: BarData | None = None,
        tick_counter: Iterator[int] | None = None,
    ) -> MarketDataMessage:
        from msgspec import convert

        from pfeed.streaming import BarMessage, TickMessage

        common = {
            "msg_ts": msg.get("ts", None),
            "data_source": data_source,
            "data_origin": data_origin,
            "product": product.name,
            "basis": str(product.basis),
            "symbol": product.symbol,
            "specs": product.specs,
            "resolution": repr(target_resolution),
        }
        if target_resolution.is_tick():
            data: dict[str, Any] = msg["data"]
            message = convert(
                {
                    **common,
                    "index": next(tick_counter) if tick_counter is not None else 0,
                    "ts": data["ts"],
                    "price": data["price"],
                    "volume": data["volume"],
                    "extra": data.get("extra", {}),
                },
                TickMessage,
            )
        elif target_resolution.is_bar():
            data: dict[str, Any] = msg["data"]
            if not data_bar:  # case when resampling is not required
                message = convert(
                    {
                        **common,
                        "ts": data.get("ts"),
                        "start_ts": data.get("start_ts"),
                        "end_ts": data.get("end_ts"),
                        "open": data["open"],
                        "high": data["high"],
                        "low": data["low"],
                        "close": data["close"],
                        "volume": data["volume"],
                        "is_incremental": data["is_incremental"],
                        "extra": data.get("extra", {}),
                    },
                    BarMessage,
                )
            # resampling: use higher resolution data (data resolution) to update data_bar (target resolution)
            else:

                def _create_bar_message(
                    data_bar: BarData, is_incremental: bool
                ) -> BarMessage:
                    return convert(
                        {
                            **common,
                            # data_bar (pfund Bar) works in float seconds; convert back
                            # to pfeed's int-ns contract before building the message.
                            "ts": seconds_to_ns(data_bar.ts),
                            "start_ts": seconds_to_ns(data_bar.start_ts),
                            "end_ts": seconds_to_ns(data_bar.end_ts),
                            "open": data_bar.open,
                            "high": data_bar.high,
                            "low": data_bar.low,
                            "close": data_bar.close,
                            "volume": data_bar.volume,
                            "is_incremental": is_incremental,
                        },
                        BarMessage,
                    )

                def _update_bar_data_by_tick(
                    data_bar: BarData, data: dict[str, Any], msg: dict[str, Any]
                ) -> None:
                    data_bar.on_update(
                        o=data["price"],
                        h=data["price"],
                        l=data["price"],
                        c=data["price"],
                        v=data["volume"],
                        # pfund Bar is seconds-based; pfeed timestamps are int ns.
                        ts=ns_to_seconds(data["ts"]),
                        # NOTE: extra data is about tick data, don't pass it to bar data
                        # extra=data.get('extra', {}),
                        is_incremental=True,
                        msg_ts=ns_to_seconds(msg.get("ts")),
                    )

                def _update_bar_data(
                    data_bar: BarData, data: dict[str, Any], msg: dict[str, Any]
                ) -> None:
                    data_bar.on_update(
                        # pfund Bar is seconds-based; pfeed timestamps are int ns.
                        start_ts=ns_to_seconds(data.get("start_ts")),
                        end_ts=ns_to_seconds(data.get("end_ts")),
                        ts=ns_to_seconds(data.get("ts")),
                        o=data["open"],
                        h=data["high"],
                        l=data["low"],
                        c=data["close"],
                        v=data["volume"],
                        msg_ts=ns_to_seconds(msg.get("ts")),
                        is_incremental=True,
                        # extra data is about the data with data resolution, don't pass it to bar data (target resolution)
                        # extra=data.get('extra', {}),
                    )

                if data_resolution.is_tick():
                    # seconds to compare against data_bar's seconds-based end_ts / time.time()
                    ts = ns_to_seconds(data.get("ts"))
                    if not data_bar.is_closed() and data_bar.is_closed(
                        now=ts or time.time()
                    ):
                        # bar is closed, finalize the message first before creating a new bar
                        message = _create_bar_message(data_bar, is_incremental=False)
                        # this will create a new bar since ts > bar's end_ts
                        _update_bar_data_by_tick(data_bar, data, msg)
                    else:
                        _update_bar_data_by_tick(data_bar, data, msg)
                        message = _create_bar_message(data_bar, is_incremental=True)
                # e.g. target resolution is '5s', data resolution is '1s' -> resample data from '1s' to '5s'
                # NOTE: data['is_incremental] is saying whether the data with **data resolution** (resampler) is incremental or not
                # it is IRRELEVANT to the target resolution, i.e. for target resolution (resamplee) it is always incremental until the bar is closed
                elif data_resolution.is_bar():
                    # seconds to compare against data_bar's seconds-based end_ts / time.time()
                    ts = ns_to_seconds(data.get("ts"))
                    msg_ts = ns_to_seconds(msg.get("ts"))
                    if not data_bar.is_closed() and data_bar.is_closed(
                        now=ts or msg_ts or time.time()
                    ):
                        message = _create_bar_message(data_bar, is_incremental=False)
                        _update_bar_data(data_bar, data, msg)
                    else:
                        _update_bar_data(data_bar, data, msg)
                        message = _create_bar_message(data_bar, is_incremental=True)
                else:
                    raise NotImplementedError(
                        f"{product.desc_str()} unexpected data resolution {data_resolution} for data bar"
                    )
        else:
            raise NotImplementedError(
                f"{product.symbol} {target_resolution} is not supported"
            )
        return message
