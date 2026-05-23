# pyright: reportAttributeAccessIssue=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
from __future__ import annotations
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from pfeed.feeds.streaming_feed_mixin import StreamingData
    from pfeed.data_handlers.base_data_handler import SourcePath

from abc import ABC, abstractmethod


class StreamingDataHandlerMixin(ABC):
    @abstractmethod
    def _standardize_streaming_msg(self, data: StreamingData) -> dict[str, Any]:
        pass

    @abstractmethod
    def write_stream(self, data: StreamingData):
        pass

    @abstractmethod
    def _build_streaming_schema(self) -> pa.Schema:
        """pa.Schema for the dict produced by _standardize_streaming_msg.

        Concrete handlers compose this from their per-resolution message schema
        (e.g. TickMessageSchema, BarMessageSchema) plus asset-type-specific
        spec columns.
        """

    @abstractmethod
    def _create_sink_path(self) -> SourcePath:
        pass
