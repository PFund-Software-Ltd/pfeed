from typing import Any, Literal

from pfund.datas.data_config import DataConfig
from pydantic import Field

from pfeed.enums import DataLayer, ExtractType
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest


class MarketFeedStreamRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.stream] = ExtractType.stream
    data_config: DataConfig | None = None
    replay_pace: float | None = Field(
        default=0,
        description="""
        Pacing between row emissions when replaying (env=BACKTEST). Ignored otherwise.
        - 0 (default): ASAP — no sleep between rows, regardless of resolution/row count.
        - >0: fixed cadence in seconds (e.g. 1.0 → one row per wall-second).
        - None: realistic — for bars, sleep one resolution period between rows; for
          ticks, sleep the timestamp difference. Opt-in only (can take hours).
        """,
    )

    def is_streaming(self) -> bool:
        return True

    def is_replaying(self) -> bool:
        from pfund.enums.env import Environment

        return self.env == Environment.BACKTEST

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
