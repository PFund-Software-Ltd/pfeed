from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

import asyncio
import logging

import pytest

from pfeed.io.parquet_io import ParquetIO
from pfeed.io.table_io import TableIO
from pfeed.streaming.lake_optimizer import LakeOptimizer

logger = logging.getLogger(__name__)


class _TableIO(TableIO):
    """Counts optimize() calls, nothing is stored."""

    def __init__(self, base_path: str, *, error: Exception | None = None):
        super().__init__(base_path)
        self.error = error
        self.optimize_calls = 0

    def optimize(self) -> None:
        self.optimize_calls += 1
        if self.error:
            raise self.error

    def vacuum(self, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _write(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def _read(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


def test_only_table_ios_are_kept(tmp_path: Path):
    io = _TableIO(str(tmp_path / "lake"))

    optimizer = LakeOptimizer(
        [io, ParquetIO(str(tmp_path / "parquet"))], interval=0, logger=logger
    )

    assert optimizer.table_ios == [io]


def test_one_io_per_catalog(tmp_path: Path):
    io, same_catalog_io, other_catalog_io = (
        _TableIO(str(tmp_path / name)) for name in ("lake", "lake", "other")
    )

    optimizer = LakeOptimizer(
        [io, same_catalog_io, other_catalog_io], interval=0, logger=logger
    )
    optimizer.optimize()

    # optimize() compacts the whole catalog, so the same catalog is optimized once
    assert optimizer.table_ios == [io, other_catalog_io]
    assert (
        io.optimize_calls,
        same_catalog_io.optimize_calls,
        other_catalog_io.optimize_calls,
    ) == (1, 0, 1)


def test_failed_optimize_is_logged_and_the_others_still_run(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    failing_io = _TableIO(
        str(tmp_path / "lake"), error=RuntimeError("database is locked")
    )
    io = _TableIO(str(tmp_path / "other"))

    with caplog.at_level(logging.ERROR, logger=logger.name):
        LakeOptimizer([failing_io, io], interval=0, logger=logger).optimize()

    assert io.optimize_calls == 1
    assert "failed to optimize" in caplog.text
    assert "database is locked" in caplog.text


@pytest.mark.asyncio
async def test_run_optimizes_every_interval_until_cancelled(tmp_path: Path):
    io = _TableIO(str(tmp_path / "lake"))
    optimizer = LakeOptimizer([io], interval=0.01, logger=logger)

    task = asyncio.create_task(optimizer.run())
    while io.optimize_calls < 2:
        await asyncio.sleep(0.01)
    _ = task.cancel()
    _ = await asyncio.wait([task])

    assert task.cancelled()


@pytest.mark.asyncio
@pytest.mark.parametrize("interval", [0, 60])
async def test_run_returns_without_an_interval_or_table_ios(
    tmp_path: Path, interval: float
):
    ios = [] if interval else [_TableIO(str(tmp_path / "lake"))]

    # returns right away instead of sleeping until cancelled
    await asyncio.wait_for(
        LakeOptimizer(ios, interval=interval, logger=logger).run(), timeout=1
    )
