from typing import Annotated

from msgspec import Meta

from pfeed.messaging.streaming_message import StreamingMessage


class TickMessage(StreamingMessage):
    price: Annotated[float, Meta(gt=0)]
    volume: Annotated[float, Meta(gt=0)]
    