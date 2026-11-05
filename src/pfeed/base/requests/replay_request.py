from typing import ClassVar, Self

from pydantic import Field, model_validator

from pfeed.base.requests.base_request import BaseRequest
from pfeed.enums import DataLayer
from pfeed.io.base_io import BaseIO


class BaseReplayRequest(BaseRequest):
    extract_type: ClassVar[str] = "replay"

    io_for_replay: BaseIO = Field(
        description="IO the data is replayed from, not for loading data to storage"
    )
    replay_pace: float | None = Field(
        default=0,
        description="""
        Pacing between the replayed messages.
        - 0 (default): ASAP, no sleep between messages.
        - >0: fixed cadence in seconds (e.g. 1.0 → one message per wall-second).
        - None: realistic, sleeps the time between consecutive messages. Opt-in only (can take hours).
        """,
    )

    @model_validator(mode="after")
    def _check_data_layer(self) -> Self:
        if self.data_layer != DataLayer.CLEANED:
            raise ValueError("Replaying only supports CLEANED data layer")
        return self
