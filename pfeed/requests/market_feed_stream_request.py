from typing import Literal, Any

from pydantic import Field

from pfund.datas.data_config import DataConfig
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer
from pfeed.storages.storage_config import StorageConfig
from pfeed._io.io_config import IOConfig
from pfeed._sinks.sink_config import SinkConfig


class MarketFeedStreamRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.stream] = ExtractType.stream
    data_config: DataConfig | None = None
    sink_config: SinkConfig | None = None
    replay_pace: float | None = Field(
        default=None,
        description="""
        Pacing between row emissions when replaying (env=BACKTEST). Ignored otherwise.
        - None (default, realistic): for bars, sleep one resolution period between
          rows (so weekend/halt gaps don't make replay wait hours); for ticks,
          sleep the timestamp difference between consecutive rows.
        - 0: ASAP — no sleep between rows.
        - >0: fixed cadence in seconds (e.g. 1.0 → one row per wall-second
          regardless of resolution). Useful for fast iteration during backtesting.
        """
    )

    def is_streaming(self) -> bool:
        return True

    def finalize_load_config(self, storage_config: StorageConfig | None, io_config: IOConfig | None) -> None:
        super().finalize_load_config(storage_config, io_config)
        if storage_config and not self.clean_data:
            raise RuntimeError("Writing raw data in streaming is not supported")

    def model_post_init(self, context: Any, /) -> None:
        from pfund.enums.env import Environment
        super().model_post_init(context)
        is_replaying = (self.env == Environment.BACKTEST)
        if is_replaying:
            if not self.storage_config:
                raise ValueError("storage config is missing, cannot retrieve data for replaying")
            if self.storage_config.data_layer != DataLayer.CLEANED:
                raise ValueError("Replaying only supports CLEANED data layer")
