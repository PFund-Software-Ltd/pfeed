from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfund.datas.resolution import Resolution
    from pfund.venues._apis.typing import ResponseData

    from pfeed.feeds.streaming_feed_mixin import RawMessage

import polars as pl
from pfund.venues.bybit.product import BybitProduct

from pfeed.feeds.market_feed import MarketFeed
from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
from pfeed.sources.bybit.market_data_model import BybitMarketDataModel
from pfeed.sources.bybit.source import BybitSource


class BybitMarketFeed(StreamingFeedMixin, MarketFeed):
    DataSource: ClassVar[type[BybitSource]] = BybitSource
    data_source: BybitSource

    DataModel: ClassVar[type[BybitMarketDataModel]] = BybitMarketDataModel
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
        # Bybit spot CSVs ship 'volume' directly (and an 'rpi' column);
        # non-spot still ships 'size'. strict=False tolerates either schema.
        RENAMING_COLS: dict[str, str] = {"size": "volume"}
        MAPPING_COLS: dict[str, int] = {"buy": 1, "sell": -1}
        return df.rename(RENAMING_COLS, strict=False).with_columns(
            # some products use "Buy"/"Sell" while some use "buy"/"sell";
            # replace_strict errors on unexpected values rather than silently producing nulls
            pl.col("side")
            .str.to_lowercase()
            .replace_strict(MAPPING_COLS, return_dtype=pl.Int8),
            # cast to float64 to pass pandera schema validation — inverse products have int volume
            pl.col("volume").cast(pl.Float64),
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
    def _parse_message(product: BybitProduct, msg: RawMessage) -> ResponseData:
        from pfund.venues.bybit._ws_apis.ws_api_base import BybitBaseWebSocketAPI
        from pfund.venues.bybit.ws_api import BybitWebSocketAPI

        assert product.category is not None, "product.category is not initialized"
        WebSocketAPI: type[BybitBaseWebSocketAPI] = BybitWebSocketAPI.APIS[
            product.category
        ]

        channel: str = msg["topic"]
        if channel.startswith("kline"):
            return WebSocketAPI._parse_candlestick(msg)
        elif channel.startswith("publicTrade"):
            return WebSocketAPI._parse_tradebook(msg)
        # TODO: handle orderbook
        # elif channel.startswith('orderbook'):
        #     return WebSocketAPI._parse_orderbook(msg)
        else:
            raise NotImplementedError(
                f"{WebSocketAPI.venue} {product.category} {channel=} is not supported"
            )

    @staticmethod
    def _normalize_timestamps(msg: ResponseData) -> ResponseData:
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
