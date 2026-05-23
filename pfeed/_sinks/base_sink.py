# pyright: reportAttributeAccessIssue=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable
if TYPE_CHECKING:
    import pyarrow as pa
    from pfeed._io.base_io import BaseIO
    from pfeed.data_handlers.base_data_handler import SourcePath

import time
from abc import ABC, abstractmethod


class BaseSink(ABC):
    def __init__(self, io: BaseIO, flush_interval: int = 100) -> None:
        self._io = io
        self._flush_interval = flush_interval
        self._buffer: list[dict[str, Any]] = []
        self._last_flush_ts: float = time.time()
        self._schema: pa.Schema | None = None
        self._partition_func: Callable[[pa.Table], pa.Table] | None = None

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def with_schema(self, schema: pa.Schema) -> None:
        self._schema = schema

    @abstractmethod
    def write(self, data: dict[str, Any], path: SourcePath) -> None:
        """Accept one standardized streaming record. Implementations buffer
        internally and flush according to their own policy."""

    @abstractmethod
    def flush(self, path: SourcePath) -> None:
        """Force buffered records to the destination. Safe to call manually;
        also invoked indirectly by write() via _maybe_flush() when the
        interval has elapsed."""

    def set_partitioning(
        self,
        partition_func: Callable[[pa.Table], pa.Table],
        partition_columns: list[pa.Field],
    ) -> None:
        if self._schema is None:
            raise RuntimeError(f"{self.name}: call with_schema() before set_partitioning()")
        self._partition_func = partition_func
        self._schema = pa.schema(list(self._schema) + list(partition_columns))

    def _maybe_flush(self, path: SourcePath) -> None:
        """Flush if the interval has elapsed since the last successful flush.
        Subclasses call this from write() after appending to the buffer."""
        if time.time() - self._last_flush_ts < self._flush_interval or not self._buffer:
            return
        if self._schema is None:
            raise RuntimeError(f"schema is missing in {self.name}")
        self.flush(path)
        self._last_flush_ts = time.time()
        self._buffer.clear()
