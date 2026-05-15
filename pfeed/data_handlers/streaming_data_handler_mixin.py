from __future__ import annotations
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.streaming.streaming_message import StreamingMessage

from abc import ABC, abstractmethod

from pfeed.enums.stream_mode import StreamMode


class StreamingDataHandlerMixin(ABC):
    _stream_writer: Any | None = None

    @abstractmethod
    def _create_stream_writer(self, stream_mode: StreamMode, flush_interval: int):
        pass

    @abstractmethod
    def _standardize_streaming_data(self, data: dict[str, Any]) -> dict[str, Any]:
        pass

    @abstractmethod
    def _write_stream(self, data: dict[str, Any] | StreamingMessage):
        pass
