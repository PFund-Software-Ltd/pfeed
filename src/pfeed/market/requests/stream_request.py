from typing import ClassVar, Self

# TODO: pfund is no longer a dependency
# from pfund.data_config import DataConfig
from pydantic import Field, model_validator

from pfeed.enums import DataLayer
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedStreamRequest(MarketFeedBaseRequest):
    extract_type: ClassVar[str] = "stream"
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
    flush_interval: float = Field(
        default=100,
        gt=0,
        description="Seconds between writes of the buffered streamed data to the io. Only used when storing streamed data.",
    )

    def is_streaming(self) -> bool:
        return True

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
