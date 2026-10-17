"""Faucet batch extraction, i.e. open_batch() and its retries. Streaming is not covered here."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from pfeed.base.request import BaseRequest

from types import SimpleNamespace

import polars as pl
import pytest

from pfeed.base.data_model import BaseDataModel
from pfeed.dataflow.faucet import Faucet
from pfeed.dataflow.retry_policy import RetryPolicy

DATA_MODEL = BaseDataModel(data_source='TEST')


def _request(retry_policy: RetryPolicy | None = None, extract_type: str = 'download') -> BaseRequest:
    # the faucet only reads the request's data_source.name, extract_type and retry_policy
    return cast('BaseRequest', SimpleNamespace(
        data_source=SimpleNamespace(name='TEST'),
        extract_type=extract_type,
        retry_policy=retry_policy or RetryPolicy(),
    ))


class _FlakyExtract:
    """Raises the given errors in order, then returns `result`."""

    def __init__(self, errors: list[Exception], result: pl.LazyFrame | None = None):
        self.errors = list(errors)
        self.result = result
        self.calls: list[tuple[Any, Any]] = []

    def __call__(self, request: BaseRequest, data_model: BaseDataModel) -> pl.LazyFrame | None:
        self.calls.append((request, data_model))
        if self.errors:
            raise self.errors.pop(0)
        return self.result


@pytest.fixture
def sleep(mocker: MockerFixture):
    return mocker.patch('pfeed.dataflow.faucet.time.sleep')


@pytest.mark.parametrize('extract_type', ['download', 'retrieve'])
def test_name_and_extract_type(extract_type: str):
    faucet = Faucet(_request(extract_type=extract_type), _FlakyExtract([]))
    assert faucet.extract_type == extract_type
    assert faucet.name == f'TEST.{extract_type}'


def test_open_batch_passes_request_and_data_model():
    lf = pl.LazyFrame({'a': [1]})
    request = _request()
    extract = _FlakyExtract([], result=lf)

    assert Faucet(request, extract).open_batch(DATA_MODEL) is lf
    assert extract.calls == [(request, DATA_MODEL)]


def test_open_batch_returns_none():
    assert Faucet(_request(), _FlakyExtract([])).open_batch(DATA_MODEL) is None


def test_default_policy_never_retries(sleep):
    extract = _FlakyExtract([TimeoutError('timeout')])

    with pytest.raises(TimeoutError):
        Faucet(_request(), extract).open_batch(DATA_MODEL)
    assert len(extract.calls) == 1
    sleep.assert_not_called()


def test_retries_then_succeeds(sleep):
    lf = pl.LazyFrame({'a': [1]})
    policy = RetryPolicy(retry_on=(TimeoutError, ConnectionError), max_retries=3)
    extract = _FlakyExtract([TimeoutError(), ConnectionError()], result=lf)

    assert Faucet(_request(policy), extract).open_batch(DATA_MODEL) is lf
    assert len(extract.calls) == 3
    assert sleep.call_count == 2


def test_retry_wait_is_backed_off_with_jitter(mocker: MockerFixture, sleep):
    """Waits wait * backoff**attempt, scaled by a jitter drawn from [0.5, 1.5]."""
    uniform = mocker.patch('pfeed.dataflow.faucet.random.uniform', return_value=1.25)
    policy = RetryPolicy(retry_on=(TimeoutError,), max_retries=3, wait=2.0, backoff=3.0)
    extract = _FlakyExtract([TimeoutError()] * 3)

    Faucet(_request(policy), extract).open_batch(DATA_MODEL)

    uniform.assert_called_with(0.5, 1.5)
    assert [c.args[0] for c in sleep.call_args_list] == pytest.approx([2.0 * 1.25, 6.0 * 1.25, 18.0 * 1.25])


def test_gives_up_after_max_retries(sleep):
    policy = RetryPolicy(retry_on=(TimeoutError,), max_retries=2)
    extract = _FlakyExtract([TimeoutError('t1'), TimeoutError('t2'), TimeoutError('t3'), TimeoutError('never')])

    with pytest.raises(TimeoutError, match='t3') as exc_info:
        Faucet(_request(policy), extract).open_batch(DATA_MODEL)
    assert len(extract.calls) == 3  # first attempt + 2 retries
    assert sleep.call_count == 2
    assert exc_info.value.__notes__ == ['TEST.download extraction of TEST failed, gave up after 2 retries']


def test_max_retries_zero_raises_on_first_failure(sleep):
    policy = RetryPolicy(retry_on=(TimeoutError,), max_retries=0)
    extract = _FlakyExtract([TimeoutError()])

    with pytest.raises(TimeoutError):
        Faucet(_request(policy), extract).open_batch(DATA_MODEL)
    assert len(extract.calls) == 1
    sleep.assert_not_called()


def test_error_not_in_retry_on_is_not_retried(sleep):
    policy = RetryPolicy(retry_on=(TimeoutError,), max_retries=3)
    extract = _FlakyExtract([ValueError('bad request'), TimeoutError()])

    with pytest.raises(ValueError, match='bad request') as exc_info:
        Faucet(_request(policy), extract).open_batch(DATA_MODEL)
    assert len(extract.calls) == 1
    sleep.assert_not_called()
    assert not hasattr(exc_info.value, '__notes__')


def test_retry_on_matches_subclasses(sleep):
    """retry_on is matched like an except clause, so subclasses are retried too."""
    policy = RetryPolicy(retry_on=(OSError,), max_retries=1)
    extract = _FlakyExtract([ConnectionResetError()])

    assert Faucet(_request(policy), extract).open_batch(DATA_MODEL) is None
    assert len(extract.calls) == 2
