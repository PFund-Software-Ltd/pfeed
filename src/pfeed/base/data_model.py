from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    from collections.abc import Callable

    from pfeed.base.data_handler import BaseDataHandler
    from pfeed.streaming.zeromq import ZeroMQChannel, ZeroMQTopic

from pfund_blocks.typing import Message
from pydantic import BaseModel, ConfigDict, model_validator


class BaseDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    DataHandler: ClassVar[type[BaseDataHandler]]
    # NOTE: the streamed messages' wire format, see ZeroMQ.register_codec()
    StreamMessage: ClassVar[type[Any]] = Message
    # msgspec's hooks for the types it doesn't support, None if the messages only hold supported types
    enc_hook: ClassVar[Callable[[Any], Any] | None] = None
    dec_hook: ClassVar[Callable[[type, Any], Any] | None] = None

    data_source: str
    data_origin: str = ""

    @model_validator(mode="after")
    def _default_data_origin(self) -> Self:
        if not self.data_origin:
            self.data_origin = self.data_source
        return self

    def is_data_origin_effective(self) -> bool:
        """
        A data_origin is not effective if it is the same as the source name.
        """
        return self.data_origin != self.data_source

    def to_zmq_route(self) -> tuple[ZeroMQChannel, ZeroMQTopic]:
        """The zmq channel and topic of a streaming dataflow's messages, e.g. to Ray workers and from the data engine to subscribers."""
        raise NotImplementedError(f"{type(self).__name__} doesn't support streaming")

    def __str__(self) -> str:
        if self.is_data_origin_effective():
            return f"{self.data_source}:{self.data_origin}"
        else:
            return f"{self.data_source}"
