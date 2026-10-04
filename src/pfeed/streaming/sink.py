from __future__ import annotations

from typing import Any

import time


class Sink:
    """Buffers streamed rows until a flush is due.

    The sink owns no IO and no resources, so it has no open/close:
    the data handler appends rows, checks is_due(), writes `rows` through its IO,
    then calls clear(). Rows are only cleared after a successful write,
    so a failed write keeps them for the next flush.
    """

    def __init__(self, flush_interval: float = 100):
        """
        Args:
            flush_interval: seconds between flushes.
                Frequent flushes write many small files, which slows down reads until the table is compacted.
        """
        if flush_interval <= 0:
            raise ValueError(f'flush_interval must be positive, got {flush_interval}')
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

    def clear(self) -> None:
        """Drops the buffered rows after they are written, and restarts the flush interval."""
        self._rows = []
        self._last_flush = time.monotonic()

    def __len__(self) -> int:
        return len(self._rows)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(flush_interval={self._flush_interval}, rows={len(self._rows)})'
