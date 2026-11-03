from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import pyarrow as pa

import time

import polars as pl


class StreamBuffer:
    """Buffers streamed rows until a flush is due.

    The buffer owns no IO and no resources, so it has no open/close:
    the data handler appends rows, checks is_due(), writes to_dataframe() through its IO,
    then calls clear(). Rows are only cleared after a successful write,
    so a failed write keeps them for the next flush.
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

    def to_dataframe(self) -> pl.DataFrame:
        """The buffered rows as a dataframe with the buffer's schema."""
        import pyarrow as pa

        return cast('pl.DataFrame', pl.from_arrow(pa.Table.from_pylist(self._rows, schema=self._schema)))

    def clear(self) -> None:
        """Drops the buffered rows after they are written, and restarts the flush interval."""
        self._rows = []
        self._last_flush = time.monotonic()

    def __len__(self) -> int:
        return len(self._rows)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(flush_interval={self._flush_interval}, rows={len(self._rows)})'
