# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportArgumentType=false, reportUnusedParameter=false, reportAttributeAccessIssue=false
from __future__ import annotations
from typing import Literal, TYPE_CHECKING, Callable, ClassVar, Any, cast, Self

if TYPE_CHECKING:
    from collections.abc import Awaitable, Coroutine, Iterator
    from narwhals.typing import IntoFrame
    from pfund.datas.data_bar import BarData
    from pfeed.sources.data_provider_source import DataProviderSource
    from pfund.entities.products.product_base import BaseProduct
    from pfeed.feeds.streaming_feed_mixin import ParsedMessage
    from pfeed.dataflow.result import RunResult
    from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
    from pfeed.requests import (
        MarketFeedDownloadRequest,
        MarketFeedRetrieveRequest,
        MarketFeedStreamRequest,
    )
    from pfeed.streaming.market_data_message import MarketDataMessage
    from pfeed.feeds.streaming_feed_mixin import WebSocketName, RawMessage, ChannelKey

import time
import datetime
from abc import ABC, abstractmethod

import polars as pl

from pfund.enums.env import Environment
from pfund.datas.resolution import Resolution
from pfeed.config import setup_logging
from pfeed.enums import MarketDataType, DataTool, StreamMode, DataCategory, DataStorage, DataSource, DataLayer
from pfeed.feeds.time_based_feed import TimeBasedFeed
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.storages.storage_config import StorageConfig
from pfeed._io.io_config import IOConfig
from pfeed._sinks.sink_config import SinkConfig
from pfeed.storages.base_storage import BaseStorage


__all__ = []


class MarketFeed(TimeBasedFeed, ABC):
    data_model_class: ClassVar[type[MarketDataModel]] = MarketDataModel
    data_domain: ClassVar[DataCategory] = DataCategory.MARKET_DATA
    data_source: DataProviderSource
    SUPPORTS_ROLLBACK_MAX_PERIOD: ClassVar[bool] = False

    @staticmethod
    @abstractmethod
    def _normalize_raw_data(df: pl.LazyFrame) -> pl.LazyFrame:
        pass

    @abstractmethod
    def _download_impl(self, data_model: MarketDataModel, data_resolution: Resolution) -> IntoFrame | None:
        pass

    async def _stream_impl(
        self,
        faucet_streaming_callback: Callable[[WebSocketName, RawMessage, ChannelKey | None], Coroutine[Any, Any, None]],
    ) -> None:
        raise NotImplementedError(f"{self.name} _stream_impl() is not implemented")

    @staticmethod
    @abstractmethod
    def _parse_message(product: BaseProduct, msg: Any) -> ParsedMessage:
        pass

    def get_supported_resolutions(self, include_resampled: bool = False) -> list[Resolution]:
        """Get all supported resolutions for batch processing for the data source.

        Args:
            include_resampled: If False (default), return only the resolutions the
                data source literally provides. If True, also include all coarser
                resolutions derivable by resampling from the finest native one —
                e.g. a source providing '1t' also "supports" '1s', '1m', '1h', '1d', etc.
        """
        native = [
            Resolution(dtype_or_resol)
            for dtype_or_resol in self.data_source.METADATA.data_categories[DataCategory.MARKET_DATA].keys()
        ]
        if not include_resampled or not native:
            return native
        non_quotes = [r for r in native if not r.is_quote()]
        # TODO: quote data is not handled yet
        if not non_quotes:
            return native
        finest = max(non_quotes)
        return sorted(set(native) | set(finest.get_lower_resolutions(exclude_quote=True)), reverse=True)

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
        DataModel = self.data_model_class
        return DataModel(
            env=env,
            data_source=self.data_source,
            data_origin=data_origin,
            product=self.data_source.create_product(product, **product_specs) if isinstance(product, str) else product,
            resolution=resolution,
            start_date=start_date,
            end_date=end_date or start_date,
        )

    def _create_data_model_from_request(self, request: MarketFeedBaseRequest) -> MarketDataModel:
        if not request.is_streaming():
            exclude = {
                "extract_type",
                "product",
                "data_resolution",
                "dataflow_per_date",
            }
        else:
            exclude = {
                "extract_type",
                "product",
                "data_resolution",
                "stream_mode",
                "flush_interval",
            }
        return self.create_data_model(
            **request.model_dump(exclude=exclude),
            product=request.product,
            resolution=request.target_resolution,
        )

    def download(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        symbol: str = "",
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        data_origin: str = "",
        dataflow_per_date: bool = True,
        clean_data: bool = True,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Download historical data from the data source.

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
            start_date: Start date. If empty, derived from `rollback_period`.
            end_date: End date. If empty, defaults to today.
            data_origin: Origin label used to distinguish data from different providers
                of the same source.
            dataflow_per_date: If True, one dataflow per date (enables parallelism).
                If False, a single dataflow spans the whole range.
            clean_data: Whether to clean raw data after download.
                Ignored when `storage_config` is provided — cleaning is then determined
                by `data_layer`. If True, runs default transformations (normalize,
                standardize columns, resample). If False, raw data is returned as-is.
            storage_config: Where to persist downloaded data. If None, data is not
                persisted to storage.
            io_config: IO format/compression and read/write/connect options. Defaults
                to parquet + snappy.
            product_specs: Extra product attributes for products that need them, e.g.
                `download(product='BTC_USDT_OPT', strike_price=10000,
                expiration='2024-01-01', option_type='CALL')`. Leave empty first and
                read the exception message to discover required keys.

        Returns:
            RunResult of the download operation.
            Returns `self` when called in pipeline mode.
        """
        from pfeed.requests import MarketFeedDownloadRequest

        assert any([start_date, end_date, rollback_period]), (
            "at least one of start_date, end_date, or rollback_period must be provided"
        )
        env = Environment.BACKTEST
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, symbol=symbol, **product_specs)
        resolution = Resolution(resolution)
        start_date, end_date = self._standardize_dates(resolution, start_date, end_date, rollback_period)
        candidates = [r for r in self.get_supported_resolutions() if r >= resolution]
        if not candidates:
            raise ValueError(f"{resolution} is not supported by {self.name}")
        # find the first resolution that is >= the target resolution
        data_resolution = min(candidates)
        if storage_config is not None:
            storage_config = self._normalize_storage_config(storage_config)
        request = MarketFeedDownloadRequest(
            storage_config=storage_config,
            io_config=io_config,
            env=env,
            product=product,
            target_resolution=resolution,
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            dataflow_per_date=dataflow_per_date,
            clean_data=clean_data,
        )
        self._requests.append(request)
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
        from pfeed._etl.base import convert_dataframe
        from pfeed.utils import lambda_with_name

        default_transformations = [
            lambda_with_name("convert_to_polars_df",
                lambda data: convert_dataframe(data, DataTool.polars)),
            lambda_with_name("standardize_date_column",
                lambda df: self._standardize_date_column(df, is_raw_data=not request.clean_data)),
        ]
        if request.clean_data:
            default_transformations.extend([
                self._normalize_raw_data,
                lambda_with_name("standardize_columns",
                    lambda df: etl.standardize_columns(df, request.product, request.data_resolution)),
                lambda_with_name("resample_data_if_necessary",
                    lambda df: etl.resample_data(df, request.target_resolution, request.product)),
                etl.organize_columns,
            ])
        default_transformations.append(
            lambda_with_name("convert_to_user_df",
                lambda df: convert_dataframe(df))
        )
        return default_transformations

    def retrieve(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        symbol: str = "",
        rollback_period: str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str = "",
        end_date: datetime.date | str = "",
        data_origin: str = "",
        env: Environment = Environment.BACKTEST,
        dataflow_per_date: bool = False,
        clean_data: bool = False,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Retrieve data from storage.

        Args:
            product: Financial product, e.g. BTC_USDT_PERP, where PERP = product type "perpetual".
            resolution: Data resolution. e.g. '1m' = 1 minute as the unit of each data bar/candle.
                For convenience, data types such as 'tick', 'second', 'minute' etc. are also supported.
            symbol: Source-specific symbol. If empty, derived from `product` — but the
                derivation may be wrong, in which case pass it explicitly.
            rollback_period: Lookback from today, only used when `start_date` is empty.
                Accepts a resolution string (e.g. '7d'), 'ytd', or 'max'. With 'max',
                the source's own `start_date` attribute is used.
            start_date: Start date. If empty, derived from `rollback_period`.
            end_date: End date. If empty, defaults to today.
            env: Trading Environment (e.g. 'BACKTEST') to retrieve data from.
            data_origin: Origin label used to distinguish data from different providers
                of the same source.
            dataflow_per_date: Whether to create a dataflow for each date.
                If False (default), retrieve all dates in a single dataflow —
                one polars multi-file scan + one resample. Fastest for typical queries.
                Set True when:
                    - The resample is too large to fit in memory (one date at a time fits).
                    - Using Ray and want per-date tasks parallelized across workers.
            clean_data: Whether to clean raw data.
                If data_layer is not RAW in storage_config, this parameter will be ignored.
                If True, raw data stored in data layer=RAW will be cleaned using the default transformations for download.
                If False, raw data stored in data layer=RAW will be loaded as is.
            storage_config: Where to retrieve the data from. If None, try to retrieve from the local storage.
            io_config: IO format/compression and read/write/connect options used to retrieve data.
                Defaults to parquet + snappy.
            product_specs: Extra product attributes for products that need them, e.g.
                `download(product='BTC_USDT_OPT', strike_price=10000,
                expiration='2024-01-01', option_type='CALL')`. Leave empty first and
                read the exception message to discover required keys.

        Returns:
            RunResult of the retrieval operation.
            Returns `self` when called in pipeline mode.
        """
        from pfeed.requests import MarketFeedRetrieveRequest

        assert any([start_date, end_date, rollback_period]), (
            "at least one of start_date, end_date, or rollback_period must be provided"
        )
        env = Environment[env.upper()]
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, symbol=symbol, **product_specs)
        resolution = Resolution(resolution)
        start_date, end_date = self._standardize_dates(resolution, start_date, end_date, rollback_period)
        # search for higher resolutions (highest first), e.g. if resolution is '1m', search '1m' -> '1t' -> '1s'
        search_resolutions = [resolution] + sorted([
            _resolution
            for _resolution in self.get_supported_resolutions(include_resampled=True)
            if _resolution > resolution
        ], reverse=True)

        storage_config = self._normalize_storage_config(storage_config or StorageConfig())
        io_config = self._normalize_io_config(io_config or IOConfig())

        # read metadata from storage to try to find data resolution
        Storage = DataStorage[storage_config.storage].storage_class
        storage = Storage.from_storage_config(storage_config).with_io(io_config)
        data_model = data_resolution = None
        for search_resolution in search_resolutions:
            data_model = self.create_data_model(
                env=env,
                product=product,
                resolution=search_resolution,
                start_date=start_date,
                end_date=end_date,
                data_origin=data_origin,
            )
            _ = storage.with_data_model(data_model)
            metadata = storage.read_metadata()
            # if this date's source path exists, data is stored at this resolution
            if not metadata.missing_source_paths:
                data_resolution = search_resolution
                break
        else:
            self.logger.debug(
                f"failed to find stored {product} data from {start_date} to {end_date} with search resolutions {search_resolutions} in {storage}"
            )

        request = MarketFeedRetrieveRequest(
            storage_config_for_retrieval=storage_config,
            io_config_for_retrieval=io_config,
            env=env,
            product=product,
            target_resolution=resolution,
            # NOTE: data_resolution could be None if data of target resolution is not found in storage
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            dataflow_per_date=dataflow_per_date,
            clean_data=clean_data,
        )
        self._requests.append(request)
        _ = self._create_batch_dataflows(
            extract_func=lambda data_model: self._retrieve_impl(data_model, storage),
        )
        return self.run() if not self.is_pipeline() else self

    def _retrieve_impl(self, data_model: MarketDataModel, storage: BaseStorage) -> pl.LazyFrame | None:
        data_resolution = cast(MarketDataModel, storage.data_model).resolution
        # data_model is of target resolution, it is important for storage to copy it
        # since it has the correct start_date and end_date when dataflow_per_date = True
        # storage should read data model with data_resolution
        _ = storage.with_data_model(data_model.model_copy(update={'resolution': data_resolution}))
        lf: pl.LazyFrame | None = storage.read()
        if lf is not None:
            self.logger.debug(f"retrived data {data_model} from {storage}")
        return lf

    def _get_default_transformations_for_retrieve(self, request: MarketFeedRetrieveRequest) -> list[Callable[..., Any]]:
        from pfeed._etl import market as etl
        from pfeed._etl.base import convert_dataframe
        from pfeed.utils import lambda_with_name

        storage_config = request.storage_config_for_retrieval

        if not request.clean_data:
            default_transformations = []
            if storage_config.data_layer != DataLayer.RAW:
                default_transformations.extend([
                    lambda_with_name("convert_to_polars_df",
                        lambda df: convert_dataframe(df, DataTool.polars)),
                    lambda_with_name("resample_data_if_necessary",
                        lambda df: etl.resample_data(df, request.target_resolution, request.product)),
                    etl.organize_columns,
                ])
            default_transformations.append(
                lambda_with_name("convert_to_user_df",
                    lambda df: convert_dataframe(df)),
            )
        else:
            # borrow download's default transformations to go through the cleaning process
            default_transformations = self._get_default_transformations_for_download(request)
        return default_transformations

    def stream(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str,
        symbol: str = "",
        rollback_period: Resolution | str | Literal["ytd", "max"] = "7d",
        start_date: datetime.date | str = "",
        end_date: datetime.date | str = "",
        callback: Callable[[WebSocketName, RawMessage], Awaitable[None] | None] | None = None,
        data_origin: str = "",
        env: Environment | str = Environment.LIVE,
        clean_data: bool = True,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
        sink_config: SinkConfig | None = None,
        **product_specs: Any,
    ) -> Self | None:
        """Stream market data, either live or by replaying historical data.

        When `start_date`, `end_date`, or `rollback_period` is provided, the stream replays
        historical data for the specified date range before continuing with live data.
        When none are provided, only live data is streamed starting from today.

        Args:
            product: Financial product, e.g. BTC_USDT_PERP, AAPL_USD_STK.
                Details of specifications should be specified in `product_specs`.
            resolution: Data resolution, e.g. '1m', '1h', 'tick'.
            symbol: Symbol used by the data source. If not specified, derived from `product`.
                Note that the derived symbol might NOT be correct, in that case, specify it manually.
            rollback_period: Period to rollback from today for historical replay.
                Only used when `start_date` is not specified. Default is '1d'.
                Accepts a resolution string (e.g. '7d'), 'ytd' (year to date), or 'max'.
            start_date: Start date for historical replay.
                If not specified, `rollback_period` is used to determine the start date.
            end_date: End date for historical replay. If not specified, uses today's date.
            callback: Async or sync callable invoked for each incoming message.
                Receives the raw message dict. If None, messages are handled by the default pipeline.
            data_origin: Origin label for the data, used to distinguish data from different sources.
            env: Trading environment. Cannot be BACKTEST. Default is LIVE.
            clean_data: Whether to clean raw streaming data.
                If storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
                If True, raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
                If False, raw data will be passed through as is.
            storage_config: Storage configuration.
                If None, streamed data will NOT be persisted to storage.
                If provided, streamed data will be stored according to the storage config.
            product_specs: Additional product specifications (e.g. strike_price, expiration for options).
                E.g. stream(product='BTC_USDT_OPT', strike_price=10000, expiration='2024-01-01', option_type='CALL').
        """
        from pfund_kit.utils.temporal import get_utc_now
        from pfeed.requests import MarketFeedStreamRequest

        env = Environment[env.upper()]
        if env == Environment.BACKTEST:
            raise ValueError("streaming is not supported in env BACKTEST")
        setup_logging(env=env)
        product: BaseProduct = self.data_source.create_product(product, symbol=symbol, **product_specs)
        resolution = Resolution(resolution)
        if any([start_date, end_date, rollback_period]):
            start_date, end_date = self._standardize_dates(resolution, start_date, end_date, rollback_period)
        else:
            today = get_utc_now().date()
            start_date = end_date = today

        data_resolution = resolution
        if resolution.is_bar():
            from pfund.datas.data_config import DataConfig
            from pfund.components.mixin import ComponentMixin
            # borrow pfund's data config to reuse its auto-resampling logic to find out data resolution
            # e.g. '1s' is not supported by bybit, '1t' will be used instead to resample data
            data_config = DataConfig(data_source=self.data_source.name, data_origin=data_origin)
            data_config.data_resolutions = [data_resolution]
            resampled_data_config = data_config.auto_resample(ComponentMixin.get_supported_resolutions(product))
            if resampled_data_config.resample != data_config.resample:
                data_resolution = resampled_data_config.resample[resolution]
                self.logger.warning(
                    f"{product.name} {resolution} is not supported in streaming, using {data_resolution} instead to resample data",
                )
        else:
            data_config = None

        request = MarketFeedStreamRequest(
            data_config=data_config,
            storage_config=storage_config,
            io_config=io_config,
            sink_config=sink_config,
            env=env,
            product=product,
            target_resolution=resolution,
            data_resolution=data_resolution,
            start_date=start_date,
            end_date=end_date,
            data_origin=data_origin,
            clean_data=clean_data,
        )
        self._requests.append(request)
        self._create_stream_dataflow(env=env, user_callback=callback)
        return self.run() if not self.is_pipeline() else self  # pyright: ignore[reportReturnType]

    # NOTE: ALL transformation functions MUST be static methods so that they can be serialized by Ray
    def _get_default_transformations_for_stream(self, request: MarketFeedStreamRequest) -> list[Callable[..., Any]]:
        from itertools import count
        from pfund.datas.data_bar import BarData
        from pfeed.utils import lambda_with_name

        default_transformations: list[Callable[..., Any]] = []
        if request.clean_data:
            # Bind concrete subclass's staticmethod into a local — no `self` captured.
            parse_message = type(self)._parse_message
            default_transformations.append(
                lambda_with_name(
                    'parse_message',
                    lambda msg: parse_message(request.product, msg)
                ),
            )
            # NOTE: cannot write self.data_source.name inside self.transform(), otherwise, "self" will be serialized by Ray and return an error
            data_source: DataSource = self.data_source.name
            tick_counter = (
                count() if cast(Resolution, request.data_resolution).is_tick() else None
            )
            is_resampling = bool(request.target_resolution < request.data_resolution)  # pyright: ignore[reportOperatorIssue]
            if is_resampling:
                # use data_bar to resample data, e.g. bybit doesn't support '1s' data (target resolution), use '1t' (data resolution) instead
                data_bar = BarData(
                    product=request.product,
                    resolution=request.target_resolution,
                    data_config=request.data_config,
                    storage_config=request.storage_config,
                )
            else:
                data_bar = None
            default_transformations.append(
                lambda_with_name(
                    "standardize_message",
                    lambda msg: MarketFeed._standardize_message(
                        data_source=data_source,
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
        data_source: DataSource,
        product: BaseProduct,
        target_resolution: Resolution,
        data_resolution: Resolution,
        msg: ParsedMessage,
        data_bar: BarData | None = None,
        tick_counter: Iterator[int] | None = None,
    ) -> MarketDataMessage:
        from msgspec import convert
        from pfeed.streaming import BarMessage, TickMessage

        common = {
            "msg_ts": msg.get("ts", None),
            "data_source": data_source.value,
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
                    "extra_data": data.get("extra_data", {}),
                },
                TickMessage,
            )
        elif target_resolution.is_bar():
            data: dict[str, Any] = msg["data"]
            if not data_bar:  # case when resampling is not required
                message = convert(
                    {
                        **common,
                        "ts": data.get("ts", None),
                        "start_ts": data.get("start_ts", None),
                        "end_ts": data.get("end_ts", None),
                        "open": data["open"],
                        "high": data["high"],
                        "low": data["low"],
                        "close": data["close"],
                        "volume": data["volume"],
                        "is_incremental": data["is_incremental"],
                        "extra_data": data.get("extra_data", {}),
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
                            "ts": data_bar.ts,
                            "start_ts": data_bar.start_ts,
                            "end_ts": data_bar.end_ts,
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
                        ts=data["ts"],
                        # NOTE: extra data is about tick data, don't pass it to bar data
                        # extra_data=data.get('extra_data', {}),
                        is_incremental=True,
                        msg_ts=msg.get("ts", None),
                    )

                def _update_bar_data(
                    data_bar: BarData, data: dict[str, Any], msg: dict[str, Any]
                ) -> None:
                    data_bar.on_update(
                        start_ts=data.get("start_ts", None),
                        end_ts=data.get("end_ts", None),
                        ts=data.get("ts", None),
                        o=data["open"],
                        h=data["high"],
                        l=data["low"],
                        c=data["close"],
                        v=data["volume"],
                        msg_ts=msg.get("ts", None),
                        is_incremental=True,
                        # extra data is about the data with data resolution, don't pass it to bar data (target resolution)
                        # extra_data=data.get('extra_data', {}),
                    )

                if data_resolution.is_tick():
                    ts = data.get("ts", None)
                    if not data_bar.is_closed() and data_bar.is_closed(now=ts or time.time()):
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
                    ts = data.get("ts", None)
                    msg_ts = msg.get("ts", None)
                    if not data_bar.is_closed() and data_bar.is_closed(now=ts or msg_ts or time.time()):
                        message = _create_bar_message(data_bar, is_incremental=False)
                        _update_bar_data(data_bar, data, msg)
                    else:
                        _update_bar_data(data_bar, data, msg)
                        message = _create_bar_message(data_bar, is_incremental=True)
                else:
                    raise NotImplementedError(
                        f"{product.name} unexpected data resolution {data_resolution} for data bar"
                    )
        else:
            raise NotImplementedError(
                f"{product.symbol} {target_resolution} is not supported"
            )
        return message
