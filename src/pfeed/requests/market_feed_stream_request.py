from typing import Any, Literal

from pfund.datas.data_config import DataConfig
from pydantic import Field

from pfeed.enums import DataLayer, ExtractType
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest


class MarketFeedStreamRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.stream] = ExtractType.stream
    data_config: DataConfig | None = None
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
        """,
    )

    def is_streaming(self) -> bool:
        return True

    def model_post_init(self, context: Any, /) -> None:
        from pfund.enums.env import Environment

        super().model_post_init(context)
        is_replaying = self.env == Environment.BACKTEST
        if is_replaying:
            if not self.storage_config:
                raise ValueError(
                    "storage config is missing, cannot retrieve data for replaying"
                )
            if self.storage_config.data_layer != DataLayer.CLEANED:
                raise ValueError("Replaying only supports CLEANED data layer")
