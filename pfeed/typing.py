from __future__ import annotations
from typing import Any, TypedDict

from pfeed.streaming.streaming_message import StreamingMessage

StreamingData = dict[str, Any] | StreamingMessage
class ParsedMessage(TypedDict):
    ts: float
    channel: str
    data: dict[str, Any]
    

__all__ = [
    "ParsedMessage",
    "StreamingData",
]