"""High-level API for getting historical/streaming data from Bybit."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

if TYPE_CHECKING:
    import datetime

    from pfund.datas.resolution import Resolution

    from pfeed.dataflow.result import RunResult
    from pfeed.feeds.streaming_feed_mixin import ParsedMessage, RawMessage
    from pfeed.storages.storage_config import StorageConfig

import polars as pl
from pfund.entities.products.product_bybit import BybitProduct

from pfeed._io.io_config import IOConfig
from pfeed.enums import MarketDataType
from pfeed.feeds.market_feed import MarketFeed
from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
from pfeed.sources.bybit.market_data_model import BybitMarketDataModel
from pfeed.sources.bybit.mixin import BybitMixin

__all__ = []


class BybitMarketFeed(StreamingFeedMixin, BybitMixin, MarketFeed):
    data_model_class: ClassVar[type[BybitMarketDataModel]] = BybitMarketDataModel
    date_columns_in_raw_data: ClassVar[list[str]] = ["timestamp"]

    @staticmethod
    def _normalize_raw_data(df: pl.LazyFrame) -> pl.LazyFrame:
        """Normalize raw Bybit DataFrame into a consistent format.

        Args:
            df: DataFrame after `_standardize_date_column`

        Returns:
            Normalized DataFrame with:
            - 'size' renamed to 'volume'
            - 'side' mapped from Buy/Sell (case-insensitive) to 1/-1
            - volume cast to float64
        """
        RENAMING_COLS: dict[str, str] = {"size": "volume"}
        MAPPING_COLS: dict[str, int] = {"buy": 1, "sell": -1}
        return df.rename(RENAMING_COLS).with_columns(
            # some products use "Buy"/"Sell" while some use "buy"/"sell";
            # replace_strict errors on unexpected values rather than silently producing nulls
            pl.col("side")
            .str.to_lowercase()
            .replace_strict(MAPPING_COLS, return_dtype=pl.Int8),
            # cast to float64 to pass pandera schema validation — inverse products have int volume
            pl.col("volume").cast(pl.Float64),
        )

    def download(
        self,
        product: str,
        resolution: Resolution | MarketDataType | str = "tick",
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        clean_data: bool = True,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Download historical data from Bybit.

        Args:
            product: Product basis (e.g. 'BTC_USDT_PERP'). For products
                with extra attributes (options, futures), pass them via `product_specs`.
            resolution: Target data resolution (e.g. '1m', '1h', '1d'). If the source
                doesn't provide this resolution natively, finer-grained source data is
                downloaded and resampled down.
            rollback_period: Lookback from today, only used when `start_date` is empty.
                Accepts a resolution string (e.g. '7d'), 'ytd', or 'max'. With 'max',
                the source's own `start_date` attribute is used.
            start_date: Start date. If empty, derived from `rollback_period`.
            end_date: End date. If empty, defaults to today.
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
        return super().download(
            product=product,
            resolution=resolution,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            dataflow_per_date=True,
            clean_data=clean_data,
            storage_config=storage_config,
            io_config=io_config,
            **product_specs,
        )

    def _download_impl(
        self, data_model: BybitMarketDataModel, data_resolution: Resolution
    ) -> pl.LazyFrame | None:
        batch_api = self.data_source.get_batch_api()
        assert data_model.start_date == data_model.end_date, (
            f"{self.name} download() only supports downloading data for a single day"
        )
        product = data_model.product
        start_date = data_model.start_date
        self.logger.debug(f"downloading {product} {data_resolution} on {start_date}")
        data = batch_api.get_data(
            product=product,
            resolution=data_resolution,
            date=start_date,
        )
        return data

    @staticmethod
    def _parse_message(product: BybitProduct, msg: RawMessage) -> ParsedMessage:
        from pfund.brokers.crypto.exchanges.bybit.ws_api import WebSocketAPI
        from pfund.brokers.crypto.exchanges.bybit.ws_api_bybit import BybitWebSocketAPI

        assert product.category is not None, "product.category is not initialized"
        BybitWebSocketAPIClass: type[BybitWebSocketAPI] = WebSocketAPI._get_api_class(
            product.category
        )
        return BybitWebSocketAPIClass._parse_message(msg)

    @staticmethod
    def _normalize_timestamps(msg: ParsedMessage) -> ParsedMessage:
        """Bybit timestamps are in milliseconds, convert to nanoseconds"""
        msg["ts"] = int(msg["ts"] * 10**6)
        data = msg["data"]
        if "ts" in data:
            data["ts"] = int(data["ts"] * 10**6)
        if "start_ts" in data:
            data["start_ts"] = int(data["start_ts"] * 10**6)
        if "end_ts" in data:
            data["end_ts"] = int(data["end_ts"] * 10**6)
        return msg
