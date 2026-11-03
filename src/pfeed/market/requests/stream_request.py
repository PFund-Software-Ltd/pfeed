from typing import Self

from pydantic import Field, model_validator

from pfeed.base.requests.stream_request import BaseStreamRequest
from pfeed.enums import DataLayer
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedStreamRequest(BaseStreamRequest, MarketFeedBaseRequest):
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
    store_incremental_bars: bool = Field(
        default=False,
        description="Whether to also store the updates of a bar before it closes. Only used when storing streamed bars.",
    )

    def is_replaying(self) -> bool:
        from pfund_blocks.enums.env import Environment

        return self.env == Environment.BACKTEST

    @model_validator(mode="after")
    def _check_replay(self) -> Self:
        if self.is_replaying():
            if not self.io:
                raise ValueError("io is missing, cannot retrieve data for replaying")
            if self.data_layer != DataLayer.CLEANED:
                raise ValueError("Replaying only supports CLEANED data layer")
        return self
