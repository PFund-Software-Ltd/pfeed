from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow as pa

from abc import ABC, abstractmethod

from pfeed.enums import StreamMode


class BaseSink(ABC):
    """
    Stateful streaming writer. A sink is a streaming-mode interface to a Storage
    destination -- it borrows the storage's location, IO, and data handler setup,
    and adds the lifecycle (open -> write* -> flush -> close) plus any bookkeeping
    state needed for buffering, batching, and crash recovery.

    The abstract contract is intentionally minimal -- only what the simplest sink
    (e.g. a periodic-flush Delta writer) can honor. Capabilities like exactly-once,
    checkpointing, watermarks, or transactional commits require *correctness*
    state that a simple sink does not have; those belong on extension interfaces
    shipped by a real streaming engine (e.g. CheckpointableSink), not on this base.

    Composition over inheritance: sink has-a storage rather than is-a storage.
    Inheriting from BaseStorage would force the sink to honor read/batch-write
    semantics it can't, while duplicating storage's fluent setup would just be the
    same fields under a different name.
    """

    def __init__(
        self,
        stream_mode: StreamMode | str = StreamMode.FAST,
        flush_interval: int = 100,  # in seconds
    ):
        self._stream_mode = stream_mode
        self._flush_interval = flush_interval

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def open(self) -> None:
        """Acquire resources (buffers, file handles, connections) and recover
        from any prior crash. Called once before the first write."""

    @abstractmethod
    def write(self, batch: pa.RecordBatch) -> None:
        """Accept one Arrow RecordBatch. Implementations may buffer internally
        and flush according to their own policy."""

    @abstractmethod
    def flush(self) -> None:
        """Force buffered records to the destination. Safe to call manually;
        also invoked by write() when the implementation's flush policy triggers."""

    @abstractmethod
    def close(self) -> None:
        """Final flush + release resources. Called once at shutdown."""
