from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

import pyarrow as pa
import pytest

from pfeed.streaming.buffer import StreamBuffer

SCHEMA = pa.schema([("ts", pa.int64())])


@pytest.fixture
def buffer(mocker: MockerFixture) -> StreamBuffer:
    _ = mocker.patch("pfeed.streaming.buffer.time.monotonic", return_value=0.0)
    return StreamBuffer(SCHEMA, flush_interval=10)


def _advance(mocker: MockerFixture, seconds: float) -> None:
    _ = mocker.patch("pfeed.streaming.buffer.time.monotonic", return_value=seconds)


@pytest.mark.parametrize("flush_interval", [0, -1])
def test_non_positive_flush_interval_raises(flush_interval: float):
    with pytest.raises(ValueError, match="flush_interval must be positive"):
        _ = StreamBuffer(SCHEMA, flush_interval=flush_interval)


def test_is_due_after_the_interval_with_rows(
    mocker: MockerFixture, buffer: StreamBuffer
):
    buffer.append({"ts": 1})
    assert not buffer.is_due()
    _advance(mocker, 10)
    assert buffer.is_due()


def test_empty_buffer_is_never_due(mocker: MockerFixture, buffer: StreamBuffer):
    _advance(mocker, 100)
    assert not buffer.is_due()


def test_drain_empties_the_buffer_and_restarts_the_interval(
    mocker: MockerFixture, buffer: StreamBuffer
):
    buffer.append({"ts": 1})
    _advance(mocker, 10)
    assert buffer.drain() == [{"ts": 1}]
    assert len(buffer) == 0
    buffer.append({"ts": 2})
    assert not buffer.is_due()


def test_restore_puts_the_rows_back_ahead_of_the_new_ones(buffer: StreamBuffer):
    buffer.append({"ts": 1})
    rows = buffer.drain()
    buffer.append({"ts": 2})
    buffer.restore(rows)
    assert buffer.rows == [{"ts": 1}, {"ts": 2}]


def test_to_dataframe_uses_the_schema(buffer: StreamBuffer):
    df = buffer.to_dataframe([{"ts": 1, "extra": "dropped"}])
    assert df.columns == ["ts"]
    assert df["ts"].to_list() == [1]
