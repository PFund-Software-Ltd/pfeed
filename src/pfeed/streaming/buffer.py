from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import pyarrow as pa

import time

import polars as pl


class StreamBuffer:
    """Buffers streamed rows until a flush is due.

    The buffer owns no IO and no resources, so it has no open/close:
    the data handler appends rows, checks is_due(), drain()s the rows and writes them through its IO,
    and restore()s them if the write fails, so they are written at the next flush.
    """

    def __init__(self, schema: pa.Schema, flush_interval: float = 100):
        """
        Args:
            schema: the schema of the buffered rows, see BaseDataHandler._build_streaming_schema().
            flush_interval: seconds between flushes.
                Frequent flushes write many small files, which slows down reads until the table is compacted.
        """
        if flush_interval <= 0:
            raise ValueError(f'flush_interval must be positive, got {flush_interval}')
        self._schema = schema
        self._flush_interval = flush_interval
        self._rows: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()

    @property
    def flush_interval(self) -> float:
        return self._flush_interval

    @property
    def rows(self) -> list[dict[str, Any]]:
        """The buffered rows, oldest first."""
        return self._rows

    def append(self, row: dict[str, Any]) -> None:
        self._rows.append(row)

    def is_due(self) -> bool:
        """True if there are rows and flush_interval has passed since the last flush."""
        return bool(self._rows) and time.monotonic() - self._last_flush >= self._flush_interval

    def to_dataframe(self, rows: list[dict[str, Any]]) -> pl.DataFrame:
        """The rows, e.g. from drain(), as a dataframe with the buffer's schema."""
        import pyarrow as pa

        return cast('pl.DataFrame', pl.from_arrow(pa.Table.from_pylist(rows, schema=self._schema)))

    def drain(self) -> list[dict[str, Any]]:
        """Removes and returns the buffered rows to be written, and restarts the flush interval.

        The interval restarts even if the write fails, so a failed write is retried at the next flush, not at every append.
        """
        rows, self._rows = self._rows, []
        self._last_flush = time.monotonic()
        return rows

    def restore(self, rows: list[dict[str, Any]]) -> None:
        """Puts back drained rows whose write failed, ahead of the rows appended since."""
        self._rows = rows + self._rows

    def __len__(self) -> int:
        return len(self._rows)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(flush_interval={self._flush_interval}, rows={len(self._rows)})'
