from __future__ import annotations
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.feeds.streaming_feed_mixin import StreamingData

from abc import ABC, abstractmethod


class StreamingDataHandlerMixin(ABC):
    @abstractmethod
    def _standardize_streaming_data(self, data: dict[str, Any]) -> dict[str, Any]:
        pass

    @abstractmethod
    def write_stream(self, data: StreamingData):
        pass
