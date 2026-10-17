"""DataFlow batch ETL, through a real Faucet and a minimal concrete handler on a real IO.

Streaming (set_stream_options, run_stream, end_stream etc.) is not covered here.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

    from pfeed.base.request import BaseRequest
    from pfeed.io.base_io import Partition

import logging
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from pfeed.base.data_handler import BaseDataHandler, BaseDataMetadata
from pfeed.base.data_model import BaseDataModel
from pfeed.dataflow.dataflow import DataFlow
from pfeed.dataflow.faucet import Faucet
from pfeed.dataflow.result import DataFlowResult
from pfeed.enums import DataLayer, FlowType
from pfeed.io.base_io import DatasetKey
from pfeed.io.parquet_io import ParquetIO

KEY = DatasetKey(namespace={'data_source': 'TEST'}, name={'dataset': 'test'}, partition_by=('product',))


class _Handler(BaseDataHandler[BaseDataModel, BaseDataMetadata]):
    Metadata = BaseDataMetadata

    def _create_dataset_key(self) -> DatasetKey:
        return KEY

    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return df

    def write_batch(self, df: pl.DataFrame) -> None:
        self._write(df, {(product,): self._create_metadata() for product in df.get_column('product').unique()})

    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, BaseDataMetadata]]:
        return self._read()


class _DataModel(BaseDataModel):
    DataHandler = _Handler


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({'ts': [1, 2, 3], 'price': [100.0, 101.0, 102.0], 'product': ['BTC', 'BTC', 'ETH']})


@pytest.fixture
def io(tmp_path: Path) -> ParquetIO:
    return ParquetIO(base_path=str(tmp_path))


def _faucet(extract_func: Any, extract_type: str = 'download') -> Faucet:
    # the faucet only reads the request's data_source.name, extract_type and retry_policy
    from pfeed.dataflow.retry_policy import RetryPolicy

    request = cast('BaseRequest', SimpleNamespace(
        data_source=SimpleNamespace(name='TEST'),
        extract_type=extract_type,
        retry_policy=RetryPolicy(),
    ))
    return Faucet(request, extract_func)


def _dataflow(
    data: pl.LazyFrame | None,
    extract_type: str = 'download',
    default_transformations: list[Any] | None = None,
) -> DataFlow:
    return DataFlow(
        faucet=_faucet(lambda request, data_model: data, extract_type=extract_type),
        data_model=_DataModel(data_source='TEST'),
        default_transformations=default_transformations,
    )


def _add_one(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(pl.col('price') + 1)


# --- attributes ---


def test_attributes(df: pl.DataFrame):
    dataflow = _dataflow(df.lazy())
    assert dataflow.name == 'TEST_DataFlow'
    assert str(dataflow) == 'TEST_DataFlow.download'
    assert dataflow.data_model == _DataModel(data_source='TEST')
    assert dataflow.faucet.extract_type == 'download'
    assert dataflow.handler is None
    assert not dataflow.has_handler()
    assert not dataflow.is_streaming()
    assert not dataflow.is_replaying()
    assert not dataflow.is_sealed()
    # a fresh dataflow hasn't run yet
    assert dataflow.result.success is False


def test_logger_is_named_by_data_source(df: pl.DataFrame):
    assert _dataflow(df.lazy())._logger.name == 'pfeed.test'


# --- transformations ---


def test_seal():
    dataflow = _dataflow(None)
    dataflow.seal()
    assert dataflow.is_sealed()


def test_add_user_transformations_to_sealed_dataflow_raises():
    dataflow = _dataflow(None)
    dataflow.seal()
    with pytest.raises(ValueError, match='sealed'):
        dataflow.add_user_transformations([_add_one])


def test_default_transformations_run_before_user_transformations(df: pl.DataFrame):
    calls: list[str] = []

    def default(data: pl.LazyFrame) -> pl.LazyFrame:
        calls.append('default')
        return data.with_columns(pl.col('price') * 2)

    def user(data: pl.LazyFrame) -> pl.LazyFrame:
        calls.append('user')
        return data.with_columns(pl.col('price') + 1)

    dataflow = _dataflow(df.lazy(), default_transformations=[default])
    dataflow.add_user_transformations([user])
    out = cast(pl.LazyFrame, dataflow._transform(df.lazy())).collect()

    assert calls == ['default', 'user']
    assert out.get_column('price').to_list() == [201.0, 203.0, 205.0]


def test_default_transformations_list_is_copied():
    """Adding user transformations doesn't mutate the feed's default list."""
    defaults = [_add_one]
    dataflow = _dataflow(None, default_transformations=defaults)
    dataflow.add_user_transformations([_add_one])
    assert defaults == [_add_one]


def test_transform_returning_none_raises(df: pl.DataFrame):
    def bad(data: pl.LazyFrame) -> None:
        return None

    dataflow = _dataflow(df.lazy(), default_transformations=[bad])
    with pytest.raises(TypeError, match="'bad' should return transformed data"):
        dataflow._transform(df.lazy())


def test_transform_without_transformations_is_identity(df: pl.DataFrame):
    lf = df.lazy()
    assert _dataflow(lf)._transform(lf) is lf


# --- setup_handler ---


def test_setup_handler(io: ParquetIO):
    dataflow = _dataflow(None)
    dataflow.setup_handler(io, DataLayer.RAW, 'test_domain')

    handler = dataflow.handler
    assert isinstance(handler, _Handler)
    assert dataflow.has_handler()
    assert handler.name == 'TEST.TEST_DOMAIN.RAW.ParquetIO'
    assert handler.sink is None  # batch dataflows don't buffer


def test_setup_handler_twice_raises(io: ParquetIO):
    dataflow = _dataflow(None)
    dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')
    with pytest.raises(ValueError, match='already set'):
        dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')


def test_setup_handler_on_sealed_dataflow_raises(io: ParquetIO):
    dataflow = _dataflow(None)
    dataflow.seal()
    with pytest.raises(ValueError, match='sealed'):
        dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')


# --- run_batch ---


def test_run_batch_on_unsealed_dataflow_raises(df: pl.DataFrame):
    """Unlike run errors, this is a wiring bug, so it raises instead of being recorded as a failed result."""
    with pytest.raises(ValueError, match='unsealed'):
        _dataflow(df.lazy()).run_batch()


def test_run_batch_without_handler_materializes_transformed_data(df: pl.DataFrame):
    dataflow = _dataflow(df.lazy(), default_transformations=[_add_one])
    dataflow.seal()

    result = dataflow.run_batch()

    assert result is dataflow.result
    assert result.success
    assert result._loader is None
    assert_frame_equal(cast(pl.LazyFrame, result.data).collect(), df.with_columns(pl.col('price') + 1))


def test_run_batch_with_handler_loads_and_reads_back_lazily(mocker: MockerFixture, io: ParquetIO, df: pl.DataFrame):
    dataflow = _dataflow(df.lazy(), default_transformations=[_add_one])
    dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')
    dataflow.seal()
    handler = cast(_Handler, dataflow.handler)
    read = mocker.spy(handler, 'read')

    result = dataflow.run_batch()

    assert result.success
    assert result._loader is not None
    read.assert_not_called()  # not read back until the data is accessed
    expected = df.with_columns(pl.col('price') + 1)
    assert_frame_equal(cast(pl.LazyFrame, result.data).collect().sort('ts'), expected, check_column_order=False)
    read.assert_called_once()
    # the data is in storage, not only in the result
    lf, metadata = handler.read()
    assert lf is not None
    assert_frame_equal(lf.collect().sort('ts'), expected, check_column_order=False)
    assert set(metadata) == {('BTC',), ('ETH',)}


def test_run_batch_loads_any_dataframe_as_polars(mocker: MockerFixture, io: ParquetIO, df: pl.DataFrame):
    """User transformations may return e.g. pandas, the handler always gets a polars DataFrame."""
    def to_pandas(data: pl.LazyFrame) -> pd.DataFrame:
        return data.collect().to_pandas()

    dataflow = _dataflow(df.lazy())
    dataflow.add_user_transformations([to_pandas])
    dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')
    dataflow.seal()
    write_batch = mocker.spy(cast(_Handler, dataflow.handler), 'write_batch')

    assert dataflow.run_batch().success
    written = write_batch.call_args.args[0]
    assert isinstance(written, pl.DataFrame)
    assert_frame_equal(written, df)


def test_run_batch_empty_data_is_loaded_but_not_read_back(mocker: MockerFixture, io: ParquetIO, df: pl.DataFrame):
    """Empty data is stored so it isn't re-downloaded, but the result keeps the empty frame itself."""
    empty = df.clear().lazy()
    dataflow = _dataflow(empty)
    dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')
    dataflow.seal()
    write_batch = mocker.spy(cast(_Handler, dataflow.handler), 'write_batch')

    result = dataflow.run_batch()

    write_batch.assert_called_once()
    assert result.success
    assert result._loader is None
    assert cast(pl.LazyFrame, result.data).collect().is_empty()


def test_run_batch_no_data_from_download_fails(df: pl.DataFrame):
    dataflow = _dataflow(None, extract_type='download')
    dataflow.seal()

    result = dataflow.run_batch()

    assert not result.success
    assert isinstance(result.error, ValueError)
    assert 'produced no data' in str(result.error)


def test_run_batch_no_data_from_retrieve_is_success():
    """Missing data in storage is a valid answer for retrieve."""
    dataflow = _dataflow(None, extract_type='retrieve')
    dataflow.seal()

    result = dataflow.run_batch()

    assert result.success
    assert result.error is None
    assert result.data is None


def test_run_batch_extract_error_is_recorded(caplog: pytest.LogCaptureFixture):
    err = RuntimeError('source down')

    def extract(request: BaseRequest, data_model: BaseDataModel) -> None:
        raise err

    dataflow = DataFlow(faucet=_faucet(extract), data_model=_DataModel(data_source='TEST'))
    dataflow.seal()

    with caplog.at_level(logging.ERROR, logger='pfeed.test'):
        result = dataflow.run_batch()

    assert not result.success
    assert result.error is err
    assert dataflow.result is result
    assert 'Error in running TEST_DataFlow.download' in caplog.text


def test_run_batch_transform_error_is_recorded(df: pl.DataFrame):
    def bad(data: pl.LazyFrame) -> None:
        return None

    dataflow = _dataflow(df.lazy(), default_transformations=[bad])
    dataflow.seal()

    result = dataflow.run_batch()

    assert not result.success
    assert isinstance(result.error, TypeError)


def test_run_batch_load_error_is_recorded(mocker: MockerFixture, io: ParquetIO, df: pl.DataFrame):
    """A write error surfaces in the result, not only in the log."""
    err = OSError('disk full')
    dataflow = _dataflow(df.lazy())
    dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')
    dataflow.seal()
    mocker.patch.object(cast(_Handler, dataflow.handler), 'write_batch', side_effect=err)

    result = dataflow.run_batch()

    assert not result.success
    assert result.error is err


@pytest.mark.parametrize('flow_type', [FlowType.native, 'native', 'NATIVE'])
def test_run_batch_flow_type_accepts_enum_or_any_case_str(df: pl.DataFrame, flow_type: FlowType | str):
    dataflow = _dataflow(df.lazy())
    dataflow.seal()
    assert dataflow.run_batch(flow_type=cast(FlowType, flow_type)).success
    assert dataflow._flow_type == FlowType.native


def test_run_batch_invalid_flow_type_raises(df: pl.DataFrame):
    dataflow = _dataflow(df.lazy())
    dataflow.seal()
    with pytest.raises(KeyError):
        dataflow.run_batch(flow_type=cast(FlowType, 'airflow'))


# --- prefect ---


def test_to_prefect_dataflow_defaults():
    dataflow = _dataflow(None)
    prefect_flow = dataflow.to_prefect_dataflow()
    assert prefect_flow.name == 'TEST_DataFlow'
    assert prefect_flow.flow_run_name == 'TEST'
    assert prefect_flow.log_prints is True


def test_to_prefect_dataflow_kwargs_override_defaults():
    dataflow = _dataflow(None)
    prefect_flow = dataflow.to_prefect_dataflow(name='custom', flow_run_name='run', log_prints=False, retries=2)
    assert prefect_flow.name == 'custom'
    assert prefect_flow.flow_run_name == 'run'
    assert prefect_flow.log_prints is False
    assert prefect_flow.retries == 2


def test_run_batch_prefect_passes_prefect_kwargs(mocker: MockerFixture, df: pl.DataFrame):
    dataflow = _dataflow(df.lazy())
    dataflow.seal()
    expected = DataFlowResult.no_data()
    to_prefect = mocker.patch.object(dataflow, 'to_prefect_dataflow', return_value=lambda: expected)

    assert dataflow.run_batch(flow_type=FlowType.prefect, prefect_kwargs={'retries': 1}) is expected
    to_prefect.assert_called_once_with(retries=1)


@pytest.fixture(scope='module')
def prefect_harness():
    from prefect.testing.utilities import prefect_test_harness

    # starts a temporary prefect server, shared by the module's tests since it takes a few seconds
    with prefect_test_harness():
        yield


@pytest.mark.usefixtures('prefect_harness')
def test_run_batch_prefect(io: ParquetIO, df: pl.DataFrame):
    dataflow = _dataflow(df.lazy(), default_transformations=[_add_one])
    dataflow.setup_handler(io, DataLayer.CLEANED, 'test_domain')
    dataflow.seal()
    handlers_before = list(dataflow._logger.handlers)

    result = dataflow.run_batch(flow_type=FlowType.prefect)

    assert result.success, result.error
    assert dataflow._flow_type == FlowType.prefect
    expected = df.with_columns(pl.col('price') + 1)
    assert_frame_equal(cast(pl.LazyFrame, result.data).collect().sort('ts'), expected, check_column_order=False)
    # prefect's log handlers are only attached during the run
    assert dataflow._logger.handlers == handlers_before


@pytest.mark.usefixtures('prefect_harness')
def test_run_batch_prefect_error_is_recorded(df: pl.DataFrame):
    def bad(data: pl.LazyFrame) -> None:
        return None

    dataflow = _dataflow(df.lazy(), default_transformations=[bad])
    dataflow.seal()

    result = dataflow.run_batch(flow_type=FlowType.prefect)

    assert not result.success
    assert isinstance(result.error, TypeError)
