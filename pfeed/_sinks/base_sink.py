# pyright: reportAttributeAccessIssue=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable
if TYPE_CHECKING:
    from pfeed._io.base_io import BaseIO
    from pfeed._sinks.sink_config import SinkConfig
    from pfeed.data_handlers.base_data_handler import SourcePath

import time
from abc import ABC, abstractmethod

import pyarrow as pa


class BaseSink(ABC):
    def __init__(self, io: BaseIO, sink_config: SinkConfig) -> None:
        self._io = io
        self._config = sink_config
        self._buffer: list[dict[str, Any]] = []
        self._last_flush_ts: float = time.time()
        self._schema: pa.Schema | None = None
        self._partition_func: Callable[[pa.Table], pa.Table] | None = None
        self._partition_columns: list[str] | None = None

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def config(self) -> SinkConfig:
        return self._config

    @property
    def flush_interval(self) -> int:
        return self._config.flush_interval

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
        partition_columns: list[str],
    ) -> None:
        self._partition_func = partition_func
        self._partition_columns = partition_columns

    def _maybe_flush(self, path: SourcePath) -> None:
        """Flush if the interval has elapsed since the last successful flush.
        Subclasses call this from write() after appending to the buffer."""
        if time.time() - self._last_flush_ts < self.flush_interval or not self._buffer:
            return
        if self._schema is None:
            raise RuntimeError(f"schema is missing in {self.name}")
        self.flush(path)
        self._last_flush_ts = time.time()
        self._buffer.clear()

    def _partition(self, table: pa.Table) -> pa.Table:
        assert self._schema is not None, "schema is missing"
        if self._partition_func is not None and self._partition_columns is not None:
            table = self._partition_func(table)
            partition_fields = [table.schema.field(n) for n in self._partition_columns]
            schema = pa.schema(list(self._schema) + partition_fields)
        else:
            schema = self._schema
        table = table.select(schema.names).cast(schema)
        return table
