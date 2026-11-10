from typing import ClassVar, Self

from pfund_blocks.enums.env import Environment
from pydantic import Field, model_validator

from pfeed.base.requests.base_request import BaseRequest
from pfeed.enums import DataLayer


class BaseStreamRequest(BaseRequest):
    extract_type: ClassVar[str] = "stream"

    env: Environment
    flush_interval: float = Field(
        default=100,
        gt=0,
        description="Seconds between writes of the buffered streamed data to the io. Only used when storing streamed data.",
    )

    @model_validator(mode="after")
    def _check_env(self) -> Self:
        if self.env not in (Environment.PAPER, Environment.LIVE):
            raise ValueError(
                f"streaming is only supported in PAPER and LIVE, got {self.env}"
            )
        return self

    @model_validator(mode="after")
    def _check_data_layer(self) -> Self:
        if self.data_layer == DataLayer.RAW:
            raise ValueError(
                "streamed data is parsed by the source, it cannot be in the RAW data layer"
            )
        return self
