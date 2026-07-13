"""Historical trade feed backed by CryptoHFTData."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

if TYPE_CHECKING:
    import datetime

    from pfund.datas.resolution import Resolution

    from pfeed._io.io_config import IOConfig
    from pfeed.dataflow.result import RunResult
    from pfeed.storages.storage_config import StorageConfig

import polars as pl

from pfeed.enums import MarketDataType
from pfeed.feeds.market_feed import MarketFeed
from pfeed.sources.crypto_hft_data.market_data_model import (
    CryptoHftDataMarketDataModel,
)
from pfeed.sources.crypto_hft_data.mixin import CryptoHftDataMixin
from pfeed.sources.crypto_hft_data.product import CryptoHftDataProduct

__all__ = []


class CryptoHftDataMarketFeed(CryptoHftDataMixin, MarketFeed):
    """Download venue-specific trades and derive standard PFeed bars."""

    data_model_class: ClassVar[type[CryptoHftDataMarketDataModel]] = (
        CryptoHftDataMarketDataModel
    )
    date_columns_in_raw_data: ClassVar[list[str]] = [
        "trade_time",
        "event_time",
        "received_time",
    ]

    @staticmethod
    def _normalize_raw_data(df: pl.LazyFrame) -> pl.LazyFrame:
        """Map CryptoHFTData trades to PFeed's standard tick schema."""
        return df.rename({"quantity": "volume"}).with_columns(
            pl.col("price").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.when(pl.col("is_buyer_maker"))
            .then(pl.lit(-1, dtype=pl.Int8))
            .otherwise(pl.lit(1, dtype=pl.Int8))
            .alias("side"),
        )

    def download(
        self,
        product: str,
        exchange: str,
        resolution: Resolution | MarketDataType | str = "tick",
        symbol: str = "",
        rollback_period: Resolution | str | Literal["ytd", "max"] = "1d",
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        clean_data: bool = True,
        storage_config: StorageConfig | None = None,
        io_config: IOConfig | None = None,
        **product_specs: Any,
    ) -> Self | RunResult:
        """Download exchange-specific historical trades or resampled bars."""
        return super().download(
            product=product,
            exchange=exchange,
            resolution=resolution,
            symbol=symbol,
            rollback_period=rollback_period,
            start_date=start_date,
            end_date=end_date,
            data_origin=exchange.lower(),
            dataflow_per_date=True,
            clean_data=clean_data,
            storage_config=storage_config,
            io_config=io_config,
            **product_specs,
        )

    def _download_impl(
        self,
        data_model: CryptoHftDataMarketDataModel,
        data_resolution: Resolution,
    ) -> pl.LazyFrame | None:
        assert data_model.start_date == data_model.end_date, (
            f"{self.name} download() only supports downloading data for a single day"
        )
        product = data_model.product
        data = self.data_source.get_batch_api().get_trades(
            symbol=product.symbol,
            exchange=product.exchange,
            start_date=data_model.start_date.isoformat(),
            end_date=data_model.end_date.isoformat(),
        )
        if data.empty:
            return None
        return pl.from_pandas(data).lazy()

    @staticmethod
    def _parse_message(product: CryptoHftDataProduct, msg: Any) -> Any:
        raise NotImplementedError("CryptoHFTData does not provide live streaming")

    @staticmethod
    def _normalize_timestamps(msg: Any) -> Any:
        raise NotImplementedError("CryptoHFTData does not provide live streaming")
