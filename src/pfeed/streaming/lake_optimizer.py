from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging
    from collections.abc import Iterable

    from pfeed.io.base_io import BaseIO
    from pfeed.io.table_io import TableIO

import asyncio
import threading


class LakeOptimizer:
    """Optimizes the table IOs (e.g. DuckLakeIO) streams store to, e.g. moves DuckLake's inlined rows into parquet files.

    Runs every interval while streaming and once when the streams end, only in the main process,
    so Ray workers don't optimize the same catalog at the same time.
    """

    def __init__(self, ios: Iterable[BaseIO], interval: float, logger: logging.Logger):
        """
        Args:
            ios: the IOs the streams store to; IOs that aren't table IOs (e.g. ParquetIO) are skipped.
            interval: seconds between optimizations while streaming, 0 optimizes only when the streams end.
        """
        from pfeed.io.table_io import TableIO

        # NOTE: one IO per catalog, optimize() compacts the whole catalog, e.g. two DuckLakeIO() with the same base_path
        table_ios: dict[tuple[type, str], TableIO] = {}
        for io in ios:
            if isinstance(io, TableIO):
                _ = table_ios.setdefault((type(io), io._base_path), io)
        self._table_ios = list(table_ios.values())
        self._interval = interval
        self._logger = logger
        # NOTE: the final optimize() waits for a periodic one still running in its thread, cancelling to_thread() doesn't stop the thread
        self._lock = threading.Lock()

    @property
    def table_ios(self) -> list[TableIO]:
        return self._table_ios

    def optimize(self) -> None:
        """Optimizes every table IO once; a failure is logged, not raised, the streamed rows are already stored, only not compacted."""
        with self._lock:
            for io in self._table_ios:
                try:
                    io.optimize()
                    self._logger.debug(f"optimized {io!r}")
                except Exception:
                    self._logger.exception(f"failed to optimize {io!r}:")

    async def run(self) -> None:
        """Optimizes every interval until cancelled, returns right away if there is nothing to optimize periodically."""
        if not self._interval or not self._table_ios:
            return
        while True:
            await asyncio.sleep(self._interval)
            # NOTE: in a thread, optimize() blocks for a while and would hold up the websocket readers
            await asyncio.to_thread(self.optimize)
