from typing import Annotated

from msgspec import Meta

from pfeed.streaming.streaming_message import StreamingMessage


class TickMessage(StreamingMessage, frozen=True):
    # monotonically increasing index to preserve tick order within the same timestamp
    # useful when multiple ticks have the same ts
    index: Annotated[int, Meta(ge=0)]
    price: Annotated[float, Meta(gt=0)]
    volume: Annotated[float, Meta(gt=0)]
    