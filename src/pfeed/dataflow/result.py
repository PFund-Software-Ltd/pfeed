from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    from narwhals.typing import IntoFrame

    from pfeed.dataflow.dataflow import DataFlow


@dataclass
class DataFlowResult:
    """Result of a single DataFlow run.

    A result is in exactly one of three states:
      - materialized: `_data` is set (retrieve flows, or download without storage)
      - lazy:         `_loader` is set; data is fetched from storage on access
                      (download flow with storage — used under Ray to avoid copying
                      large frames back from worker to main process)
      - failed:       `error` is set; `success` is False

    Construct via the factory classmethods rather than the bare constructor.
    """

    success: bool = False
    error: BaseException | None = None
    _data: IntoFrame | None = None
    _loader: Callable[[], pl.LazyFrame | None] | None = None

    @classmethod
    def materialized(cls, data: IntoFrame | None) -> DataFlowResult:
        # NOTE: an empty dataframe still counts as success — the source returned a valid (empty) answer.
        return cls(success=data is not None, _data=data)

    @classmethod
    def lazy(cls, loader: Callable[[], pl.LazyFrame | None]) -> DataFlowResult:
        return cls(success=True, _loader=loader)

    @classmethod
    def failed(cls, error: BaseException | None = None) -> DataFlowResult:
        return cls(success=False, error=error)

    @property
    def data(self) -> IntoFrame | None:
        """Lazy-loads data from storage on first access.

        Data is only read from disk when this property is accessed, not when the
        DataFlowResult is created. This prevents large datasets from being held in
        memory unnecessarily, which is critical when using Ray for parallel processing.
        """
        if self._data is None and self._loader is not None:
            self._data = self._loader()
        return self._data


@dataclass(frozen=True)
class RunResult:
    """Aggregate result of a `feed.run()` call.

    Wraps the combined frame plus the underlying per-flow `DataFlowResult`s so callers
    can introspect failures programmatically instead of relying on log output.
    """

    data: IntoFrame | bytes | None
    dataflows: list[DataFlow]

    @property
    def success(self) -> bool:
        return not self.failed

    @property
    def completed(self) -> list[DataFlow]:
        return [df for df in self.dataflows if df.result.success]

    @property
    def failed(self) -> list[DataFlow]:
        return [df for df in self.dataflows if not df.result.success]

    @property
    def errors(self) -> list[BaseException]:
        return [df.result.error for df in self.failed if df.result.error is not None]
