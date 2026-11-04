"""BaseDataHandler tests, through a minimal concrete handler on a real IO.

Backend behavior is covered by tests/unit/io/test_io_contract.py; these only cover what the handler adds on top,
i.e. normalizing its inputs and (de)serializing the per-partition metadata.
Streaming is only covered for flushing the stream buffer, with the IO write mocked.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture, MockType

    from pfeed.io.base_io import Partition

import datetime

import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

from pfeed.base.data_handler import BaseDataHandler, BaseDataMetadata
from pfeed.base.data_model import BaseDataModel
from pfeed.enums import DataLayer
from pfeed.io.base_io import DatasetKey
from pfeed.io.parquet_io import ParquetIO
from pfeed.streaming.buffer import StreamBuffer

KEY = DatasetKey(namespace={'data_source': 'TEST'}, name={'dataset': 'test'}, partition_by=('product',))
D1 = datetime.date(2025, 1, 1)


class _Metadata(BaseDataMetadata):
    # a date isn't JSON-native, so it checks that metadata is dumped to JSON and parsed back to a date
    as_of: datetime.date = D1
    count: int = 0


class _DataModel(BaseDataModel):
    pass


class _Handler(BaseDataHandler[_DataModel, _Metadata]):
    Metadata = _Metadata

    def _create_dataset_key(self) -> DatasetKey:
        return KEY

    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return df

    def write_batch(self, df: pl.DataFrame) -> None:
        self._write(df, {(product,): self._create_metadata() for product in df.get_column('product').unique()})

    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, _Metadata]]:
        return self._read()


@pytest.fixture
def io(tmp_path: Path) -> ParquetIO:
    return ParquetIO(base_path=str(tmp_path))


@pytest.fixture
def handler(io: ParquetIO) -> _Handler:
    return _Handler(_DataModel(data_source='TEST'), io, data_domain='test_domain')


@pytest.fixture
def df() -> pl.DataFrame:
    """3 rows across 2 partitions: BTC (ts 1, 2) and ETH (ts 3)."""
    return pl.DataFrame({'ts': [1, 2, 3], 'price': [100.0, 101.0, 102.0], 'product': ['BTC', 'BTC', 'ETH']})


@pytest.mark.parametrize('data_layer', [DataLayer.RAW, 'raw', 'RAW', 'Raw'])
def test_data_layer_accepts_enum_or_any_case_str(io: ParquetIO, data_layer: DataLayer | str):
    handler = _Handler(_DataModel(data_source='TEST'), io, data_domain='test_domain', data_layer=data_layer)
    assert handler.name.split('.')[2] == 'RAW'


def test_data_layer_defaults_to_cleaned(handler: _Handler):
    assert handler.name.split('.')[2] == 'CLEANED'


def test_name(handler: _Handler):
    """data_domain is upper-cased; the IO is named by its class."""
    assert handler.name == 'TEST.TEST_DOMAIN.CLEANED.ParquetIO'


def test_dataset_key_is_created_by_subclass(handler: _Handler):
    assert handler.dataset_key == KEY


def test_create_metadata_defaults(handler: _Handler):
    assert handler._create_metadata() == _Metadata()


def test_read_missing_dataset(handler: _Handler):
    assert handler.read() == (None, {})


def test_metadata_roundtrip(handler: _Handler, df: pl.DataFrame):
    """Metadata is written as JSON and read back as Metadata instances, with the original types."""
    metadata: dict[Partition, _Metadata] = {('BTC',): _Metadata(as_of=datetime.date(2025, 6, 30), count=2), ('ETH',): _Metadata(count=1)}

    handler._write(df, metadata)
    lf, read_metadata = handler.read()

    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), df, check_column_order=False)
    assert read_metadata == metadata
    assert all(isinstance(md, _Metadata) for md in read_metadata.values())
    assert read_metadata[('BTC',)].as_of == datetime.date(2025, 6, 30)


def test_write_batch_roundtrip(handler: _Handler, df: pl.DataFrame):
    """write_batch() through _create_metadata(), every partition gets the default metadata."""
    handler.write_batch(df)
    _, read_metadata = handler.read()

    assert read_metadata == {('BTC',): _Metadata(), ('ETH',): _Metadata()}


def test_read_partition_subset(handler: _Handler, df: pl.DataFrame):
    handler.write_batch(df)
    lf, read_metadata = handler._read(partitions=[('ETH',)])

    assert lf is not None
    assert lf.collect().get_column('ts').to_list() == [3]
    assert read_metadata == {('ETH',): _Metadata()}


def test_parse_metadata_ignores_unknown_keys(handler: _Handler):
    """Partitions written with a metadata field that no longer exists stay readable."""
    assert handler._parse_metadata({'as_of': '2025-01-02', 'removed_field': 'x'}) == _Metadata(
        as_of=datetime.date(2025, 1, 2)
    )


def test_parse_metadata_fills_missing_keys_with_defaults(handler: _Handler):
    """Partitions written before a metadata field was added stay readable."""
    assert handler._parse_metadata({}) == _Metadata()


# --- flushing the stream buffer ---


@pytest.fixture
def streaming_handler(mocker: MockerFixture, handler: _Handler) -> _Handler:
    """A handler whose stream buffer is always due once it has rows, and whose stream writes are mocked."""
    handler._stream_buffer = StreamBuffer(pa.schema([('ts', pa.int64())]), flush_interval=1)
    _ = mocker.patch.object(handler._stream_buffer, 'is_due', side_effect=lambda: bool(handler._stream_buffer and handler._stream_buffer.rows))
    _ = mocker.patch.object(handler, '_stream_partitions', return_value={})
    _ = mocker.patch.object(handler, '_write')
    _ = mocker.patch.object(handler, '_logger')
    return handler


def _buffer(handler: _Handler) -> StreamBuffer:
    assert handler._stream_buffer is not None
    return handler._stream_buffer


def _write(handler: _Handler) -> MockType:
    return cast("MockType", handler._write)


def test_flush_writes_and_empties_the_buffer(streaming_handler: _Handler):
    _buffer(streaming_handler).append({'ts': 1})
    streaming_handler.flush()
    df = _write(streaming_handler).call_args.args[0]
    assert df['ts'].to_list() == [1]
    assert len(_buffer(streaming_handler)) == 0


def test_failed_flush_raises_and_keeps_the_rows(streaming_handler: _Handler):
    _write(streaming_handler).side_effect = RuntimeError('database is locked')
    _buffer(streaming_handler).append({'ts': 1})
    with pytest.raises(RuntimeError, match='database is locked'):
        streaming_handler.flush()
    assert _buffer(streaming_handler).rows == [{'ts': 1}]


def test_teardown_streaming_writes_the_rows_and_drops_the_buffer(streaming_handler: _Handler):
    _buffer(streaming_handler).append({'ts': 1})
    streaming_handler.teardown_streaming()
    assert _write(streaming_handler).call_args.args[0]['ts'].to_list() == [1]
    assert streaming_handler._stream_buffer is None


def test_failed_teardown_streaming_keeps_the_buffer_and_its_rows(streaming_handler: _Handler):
    _write(streaming_handler).side_effect = RuntimeError('database is locked')
    buffer = _buffer(streaming_handler)
    buffer.append({'ts': 1})
    with pytest.raises(RuntimeError, match='database is locked'):
        streaming_handler.teardown_streaming()
    assert streaming_handler._stream_buffer is buffer
    assert buffer.rows == [{'ts': 1}]


def test_flush_if_due_retries_a_failed_write_at_the_next_flush(streaming_handler: _Handler):
    _write(streaming_handler).side_effect = [RuntimeError('database is locked'), None]
    _buffer(streaming_handler).append({'ts': 1})
    streaming_handler.flush_if_due()
    cast("MockType", streaming_handler._logger).exception.assert_called_once()
    _buffer(streaming_handler).append({'ts': 2})
    streaming_handler.flush_if_due()
    df = _write(streaming_handler).call_args.args[0]
    assert df['ts'].to_list() == [1, 2]
    assert len(_buffer(streaming_handler)) == 0


def test_flush_if_due_raises_after_max_consecutive_failures(streaming_handler: _Handler):
    _write(streaming_handler).side_effect = RuntimeError('disk full')
    _buffer(streaming_handler).append({'ts': 1})
    for _ in range(streaming_handler.MAX_FAILED_FLUSHES - 1):
        streaming_handler.flush_if_due()
    with pytest.raises(RuntimeError, match='disk full'):
        streaming_handler.flush_if_due()


def test_successful_flush_resets_the_failure_count(streaming_handler: _Handler):
    errors = [RuntimeError('database is locked')] * (streaming_handler.MAX_FAILED_FLUSHES - 1)
    _write(streaming_handler).side_effect = [*errors, None, *errors]
    _buffer(streaming_handler).append({'ts': 1})
    for _ in range(2 * streaming_handler.MAX_FAILED_FLUSHES - 1):
        streaming_handler.flush_if_due()


def test_flush_if_due_does_nothing_when_not_due(streaming_handler: _Handler):
    streaming_handler.flush_if_due()
    _write(streaming_handler).assert_not_called()
