"""DataFlowResult states and the RunResult aggregate over dataflows' results."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pfeed.dataflow.dataflow import DataFlow

from types import SimpleNamespace

import polars as pl
import pytest

from pfeed.dataflow.result import DataFlowResult, RunResult


def test_default_constructor_is_unsuccessful():
    result = DataFlowResult()
    assert result.success is False
    assert result.error is None
    assert result.data is None


def test_materialized():
    df = pl.DataFrame({'a': [1]})
    result = DataFlowResult.materialized(data=df)
    assert result.success is True
    assert result.error is None
    assert result.data is df


def test_materialized_empty_dataframe_is_success():
    """An empty frame is a valid answer from the source, unlike None."""
    result = DataFlowResult.materialized(data=pl.DataFrame())
    assert result.success is True


def test_materialized_bytes():
    result = DataFlowResult.materialized(data=b'blob')
    assert result.success is True
    assert result.data == b'blob'


def test_materialized_none_is_unsuccessful():
    result = DataFlowResult.materialized(data=None)
    assert result.success is False
    assert result.data is None


def test_lazy_loads_on_first_access_only():
    lf = pl.LazyFrame({'a': [1]})
    calls = []

    def loader() -> pl.LazyFrame:
        calls.append(1)
        return lf

    result = DataFlowResult.lazy(loader=loader)
    assert result.success is True
    assert calls == []  # not loaded at creation

    assert result.data is lf
    assert result.data is lf
    assert calls == [1]  # cached after the first access


def test_lazy_loader_returning_none_is_retried():
    """None isn't cached, so a loader that returned None is called again on the next access."""
    calls = []

    def loader() -> None:
        calls.append(1)

    result = DataFlowResult.lazy(loader=loader)
    assert result.data is None
    assert result.data is None
    assert calls == [1, 1]


def test_no_data():
    result = DataFlowResult.no_data()
    assert result.success is True
    assert result.error is None
    assert result.data is None


@pytest.mark.parametrize('error', [ValueError('boom'), None])
def test_failed(error: BaseException | None):
    result = DataFlowResult.failed(error=error)
    assert result.success is False
    assert result.error is error
    assert result.data is None


def _dataflow(result: DataFlowResult) -> DataFlow:
    # RunResult only reads dataflow.result
    return cast('DataFlow', SimpleNamespace(result=result))


def test_run_result_all_succeeded():
    ok1, ok2 = _dataflow(DataFlowResult.materialized(pl.DataFrame())), _dataflow(DataFlowResult.no_data())
    run_result = RunResult(data=None, dataflows=[ok1, ok2])

    assert run_result.success is True
    assert run_result.completed == [ok1, ok2]
    assert run_result.failed == []
    assert run_result.errors == []


def test_run_result_partial_failure():
    err = RuntimeError('boom')
    ok = _dataflow(DataFlowResult.no_data())
    failed_with_error = _dataflow(DataFlowResult.failed(error=err))
    failed_without_error = _dataflow(DataFlowResult.failed())
    run_result = RunResult(data=None, dataflows=[ok, failed_with_error, failed_without_error])

    assert run_result.success is False
    assert run_result.completed == [ok]
    assert run_result.failed == [failed_with_error, failed_without_error]
    # failures without an error are skipped
    assert run_result.errors == [err]


def test_run_result_no_dataflows_is_success():
    assert RunResult(data=None, dataflows=[]).success is True

