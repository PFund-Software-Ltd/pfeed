"""BaseFeed, through a minimal concrete feed with real requests, dataflows and a real IO.

Streaming (stream requests, run_stream etc.) is not covered here.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from pytest_mock import MockerFixture

    from pfeed.io.base_io import BaseIO, Partition

import logging
from abc import ABC

import polars as pl
import pytest
from pydantic import HttpUrl

from pfeed.base.data_handler import BaseDataHandler, BaseDataMetadata
from pfeed.base.data_model import BaseDataModel
from pfeed.base.feed import BaseFeed, requires
from pfeed.base.request import BaseRequest
from pfeed.dataflow.dataflow import DataFlow
from pfeed.enums import (
    DataAccessType,
    DataCategory,
    DataLayer,
    DataProviderType,
    FlowType,
)
from pfeed.io.base_io import DatasetKey, IOCapabilities
from pfeed.io.parquet_io import ParquetIO
from pfeed.source import BaseSource, SourceMetadata

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


class _Source(BaseSource):
    Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]] = {}
    metadata = SourceMetadata(
        name='TEST',
        url=HttpUrl('https://example.com'),
        data_categories={},
        provider_type=DataProviderType.VENUE,
        access_type=DataAccessType.FREE,
    )


class _DownloadRequest(BaseRequest):
    extract_type: ClassVar[str] = 'download'

    def to_data_model(self) -> BaseDataModel:
        return _DataModel(data_source=self.data_source.name)


class _RetrieveRequest(_DownloadRequest):
    extract_type: ClassVar[str] = 'retrieve'


class _StreamRequest(_DownloadRequest):
    extract_type: ClassVar[str] = 'stream'

    def is_streaming(self) -> bool:
        return True


def _extract(request: BaseRequest, data_model: BaseDataModel) -> pl.LazyFrame | None:
    """One row per data model, its product is the data model's data_origin. Origin 'FAIL' raises, 'EMPTY' has no data."""
    if data_model.data_origin == 'FAIL':
        raise RuntimeError('extract failed')
    if data_model.data_origin == 'EMPTY':
        return None
    return pl.LazyFrame({'product': [data_model.data_origin], 'price': [1.0]})


def _add_one(df: pl.LazyFrame) -> pl.LazyFrame:
    return df.with_columns(pl.col('price') + 1)


class _FeedBase(BaseFeed[_Source, BaseRequest, _DataModel], ABC):
    """Framework base class: lists ABC directly, so it holds defaults and isn't checked by __init_subclass__."""

    DataSource = _Source
    DataModel = _DataModel
    data_domain = DataCategory.MARKET_DATA
    _required_methods: ClassVar[dict[str, tuple[str, ...]]] = {
        'download': ('_download_impl',),
        'stream': ('_stream_impl',),
    }

    class Requests:
        download: ClassVar[type[BaseRequest] | None] = _DownloadRequest
        retrieve: ClassVar[type[BaseRequest] | None] = _RetrieveRequest
        stream: ClassVar[type[BaseRequest] | None] = None

    def _download_impl(self, request: BaseRequest, data_model: BaseDataModel) -> pl.LazyFrame | None:
        raise NotImplementedError

    @classmethod
    def create_data_model(cls, *args: Any, **kwargs: Any) -> BaseDataModel:
        return _DataModel(data_source='TEST')

    def _get_default_transformations(self, request: BaseRequest, /) -> list[Callable[..., Any]]:
        return []

    def run(self, use_prefect: bool = False, **prefect_kwargs: Any) -> list[DataFlow]:
        return self._run_batch_dataflows(use_prefect, prefect_kwargs)

    @requires('download')
    def download(
        self, *data_origins: str, io: BaseIO | None = None, data_layer: DataLayer | str = DataLayer.CLEANED
    ) -> _FeedBase:
        """Creates a download request with one dataflow per data origin."""
        request = _DownloadRequest(data_source=self.data_source, io=io, data_layer=data_layer)
        self._append_request(request)
        data_models = [_DataModel(data_source='TEST', data_origin=origin) for origin in data_origins]
        self._create_batch_dataflows(extract_func=_extract, data_models=data_models)
        return self

    @requires('retrieve')
    def retrieve(self) -> str:
        return 'retrieved'


class _Feed(_FeedBase):
    def _download_impl(self, request: BaseRequest, data_model: BaseDataModel) -> pl.LazyFrame | None:
        return _extract(request, data_model)


class _NoConcurrentWritesIO(ParquetIO):
    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities(append=False, concurrent_partition_writes=False)


@pytest.fixture
def feed() -> _Feed:
    return _Feed(_Source())


@pytest.fixture
def io(tmp_path: Path) -> ParquetIO:
    return ParquetIO(base_path=str(tmp_path))


# --- requires ---


def test_requires_runs_supported_verb(feed: _Feed):
    assert feed.retrieve() == 'retrieved'


def test_requires_rejects_unsupported_verb_before_running_the_body():
    called = []

    class Feed(_Feed):
        class Requests(_Feed.Requests):
            retrieve = None
            download = None

        @requires('retrieve')
        def retrieve(self) -> str:
            called.append(1)
            return 'retrieved'

    with pytest.raises(NotImplementedError, match='TEST does not support retrieve for MARKET_DATA'):
        Feed(_Source()).retrieve()
    assert called == []


def test_requires_keeps_the_wrapped_function_name():
    assert _Feed.download.__name__ == 'download'


# --- __init_subclass__ ---


def test_framework_base_is_not_checked():
    # _FeedBase only has the default _download_impl, it would fail the check if it were checked
    class Base(_FeedBase, ABC):
        _required_methods: ClassVar[dict[str, tuple[str, ...]]] = {'download': ('_missing',)}

    assert Base.capabilities() == ['download', 'retrieve']


def test_concrete_feed_missing_required_method():
    with pytest.raises(TypeError, match="'download' needs _missing") as exc_info:
        class Feed(_Feed):
            _required_methods: ClassVar[dict[str, tuple[str, ...]]] = {'download': ('_download_impl', '_missing')}
    assert 'Feed supports' in str(exc_info.value)
    assert 'set the verbs to None in Feed.Requests' in str(exc_info.value)


def test_default_from_framework_base_counts_as_missing():
    with pytest.raises(TypeError, match="'download' needs _download_impl"):
        class Feed(_FeedBase):
            pass


def test_required_method_inherited_from_concrete_feed_is_enough():
    class Feed(_Feed):
        pass

    assert Feed.capabilities() == ['download', 'retrieve']


def test_unsupported_verb_needs_nothing():
    # download is off, so its required _download_impl isn't needed
    class Feed(_FeedBase):
        class Requests(_FeedBase.Requests):
            download = None

    assert Feed.capabilities() == ['retrieve']


def test_stream_needs_streaming_feed_mixin():
    with pytest.raises(TypeError, match="'stream' needs StreamingFeedMixin as a base class, _stream_impl"):
        class Feed(_Feed):
            class Requests(_Feed.Requests):
                stream = _StreamRequest


def test_download_needs_retrieve():
    with pytest.raises(TypeError, match=r"'download' needs Requests\.retrieve"):
        class Feed(_Feed):
            class Requests(_Feed.Requests):
                retrieve = None


def test_download_without_retrieve_verb_in_requests_is_allowed():
    # a feed family without a retrieve verb at all, e.g. an engine feed, isn't required to have it
    class Feed(_Feed):
        class Requests:
            download: ClassVar[type[BaseRequest] | None] = _DownloadRequest

    assert Feed.capabilities() == ['download']


def test_all_errors_are_reported_together():
    with pytest.raises(TypeError) as exc_info:
        class Feed(_FeedBase):
            class Requests(_FeedBase.Requests):
                stream = _StreamRequest
    msg = str(exc_info.value)
    assert "'download' needs _download_impl" in msg
    assert "'stream' needs StreamingFeedMixin as a base class, _stream_impl" in msg


# --- __init__ and attributes ---


def test_attributes(feed: _Feed):
    assert feed.name == 'TEST'
    assert isinstance(feed.data_source, _Source)
    assert feed.logger.name == 'pfeed.test'
    assert not feed.is_pipeline()
    assert not feed.is_running()
    assert not feed._is_using_ray()
    assert feed.dataflows == []
    assert feed.streaming_dataflows == []


def test_init_options():
    feed = _Feed(_Source(), pipeline_mode=True, num_workers=2)
    assert feed.is_pipeline()
    assert feed._is_using_ray()


def test_init_rejects_other_source():
    class OtherSource(BaseSource):
        Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]] = {}
        metadata = _Source.metadata

    with pytest.raises(TypeError, match='_Feed belongs to _Source, got OtherSource'):
        _Feed(cast('_Source', OtherSource()))


def test_capabilities_are_sorted_and_skip_none_and_private():
    class Feed(_Feed):
        class Requests(_Feed.Requests):
            _private = _DownloadRequest
            stream = None

    assert Feed.capabilities() == ['download', 'retrieve']
    assert Feed(_Source())._supports('download')
    assert not Feed(_Source())._supports('stream')


# --- set_num_workers / running state ---


@pytest.mark.parametrize('num_workers', [None, 1, 64])
def test_set_num_workers(feed: _Feed, num_workers: int | None):
    feed.set_num_workers(num_workers)
    assert feed._num_workers == num_workers
    assert feed._is_using_ray() is (num_workers is not None)


@pytest.mark.parametrize('num_workers', [0, -1])
def test_set_num_workers_must_be_positive(feed: _Feed, num_workers: int):
    with pytest.raises(ValueError, match='num_workers must be greater than 0'):
        feed.set_num_workers(num_workers)


def test_set_num_workers_while_running(feed: _Feed):
    feed._set_running(True)
    with pytest.raises(RuntimeError, match='cannot set num_workers while TEST is running'):
        feed.set_num_workers(2)


def test_set_running_twice_raises(feed: _Feed):
    feed._set_running(True)
    with pytest.raises(RuntimeError, match='already running'):
        feed._set_running(True)
    feed._set_running(False)
    assert not feed.is_running()


# --- requests ---


def test_get_current_request_without_request(feed: _Feed):
    with pytest.raises(AssertionError, match='No current request found'):
        feed._get_current_request()


def test_current_request_is_the_latest(feed: _Feed):
    feed.download('A').download('B')
    first, second = feed._requests
    assert feed._get_current_request() is second
    assert first is not second


@pytest.mark.parametrize(
    ('first', 'second', 'match'),
    [
        (_DownloadRequest, _StreamRequest, 'streaming request is incompatible with the queued batch requests'),
        (_StreamRequest, _DownloadRequest, 'batch request is incompatible with the queued streaming requests'),
    ],
)
def test_cannot_mix_streaming_and_batch_requests(
    feed: _Feed, first: type[BaseRequest], second: type[BaseRequest], match: str
):
    feed._append_request(first(data_source=feed.data_source))
    with pytest.raises(ValueError, match=match):
        feed._append_request(second(data_source=feed.data_source))
    assert len(feed._requests) == 1


# --- create batch dataflows ---


def test_create_batch_dataflows(feed: _Feed, mocker: MockerFixture):
    mocker.patch.object(feed, '_get_default_transformations', return_value=[_add_one])
    feed.download('A', 'B')
    request = feed._get_current_request()
    dataflows = feed._dataflows[request]

    assert [df.data_model.data_origin for df in dataflows] == ['A', 'B']
    assert feed.dataflows == dataflows
    # one faucet per request, shared by its dataflows
    assert dataflows[0].faucet is dataflows[1].faucet
    assert dataflows[0].faucet._request is request
    # default transformations run first, user transformations are added by transform()
    assert dataflows[0]._transformations == [_add_one]


def test_dataflows_across_requests(feed: _Feed):
    feed.download('A').download('B', 'C')
    assert [df.data_model.data_origin for df in feed.dataflows] == ['A', 'B', 'C']


# --- transform ---


def test_transform_targets_the_current_request_only(feed: _Feed):
    feed.download('A').download('B')
    first, second = feed.dataflows

    assert feed.transform(_add_one, _add_one) is feed
    assert first._transformations == []
    assert second._transformations == [_add_one, _add_one]


def test_transform_without_request(feed: _Feed):
    with pytest.raises(AssertionError, match='No current request found'):
        feed.transform(_add_one)


# --- load ---


def test_load_sets_up_handlers(feed: _Feed, io: ParquetIO):
    feed.download('A', 'B')
    assert feed.load(io) is feed
    request = feed._get_current_request()

    assert request.io is io
    for dataflow in feed.dataflows:
        assert dataflow.handler is not None
        assert dataflow.handler.io is io
        assert dataflow.handler._data_layer == DataLayer.CLEANED
        assert dataflow.handler._data_domain == 'MARKET_DATA'


def test_load_defaults_to_the_requests_io(feed: _Feed, io: ParquetIO):
    feed.download('A', io=io).load()
    assert all(df.handler is not None and df.handler.io is io for df in feed.dataflows)


def test_load_without_any_io_stores_nothing(feed: _Feed):
    feed.download('A').load()
    assert feed._get_current_request().io is None
    assert not any(df.has_handler() for df in feed.dataflows)


@pytest.mark.parametrize('data_layer', ['curated', 'CURATED', DataLayer.CURATED])
def test_load_to_a_higher_data_layer(feed: _Feed, io: ParquetIO, data_layer: DataLayer | str):
    feed.download('A').load(io, data_layer=data_layer)
    request = feed._get_current_request()
    handler = feed.dataflows[0].handler
    assert handler is not None
    assert handler._data_layer == DataLayer.CURATED
    # the request's own data layer is unchanged
    assert request.data_layer == DataLayer.CLEANED


def test_load_to_a_lower_data_layer(feed: _Feed, io: ParquetIO):
    feed.download('A')
    with pytest.raises(ValueError, match='cannot store CLEANED data in a lower layer RAW'):
        feed.load(io, data_layer='raw')


def test_load_targets_the_current_request_only(feed: _Feed, io: ParquetIO):
    feed.download('A').download('B').load(io)
    first, second = feed.dataflows
    assert not first.has_handler()
    assert second.has_handler()


def test_load_twice_raises(feed: _Feed, io: ParquetIO):
    feed.download('A').load(io)
    with pytest.raises(ValueError, match='handler is already set'):
        feed.load(io)


# --- prepare / finalize run ---


def test_finalize_run_loads_and_seals_every_request(feed: _Feed, io: ParquetIO, tmp_path: Path):
    other_io = ParquetIO(base_path=str(tmp_path / 'other'))
    # first request: load() not called, falls back to the request's io
    # second request: loaded explicitly
    # third request: no io anywhere
    feed.download('A', io=io).download('B').load(other_io).download('C')
    feed._finalize_run()
    a, b, c = feed.dataflows

    assert all(df.is_sealed() for df in (a, b, c))
    assert a.handler is not None and a.handler.io is io
    assert b.handler is not None and b.handler.io is other_io
    assert c.handler is None


def test_validate_io_for_ray(feed: _Feed, tmp_path: Path):
    feed.download('A').load(_NoConcurrentWritesIO(base_path=str(tmp_path)))
    # without Ray, any io is fine
    feed._validate_io_for_ray()

    feed.set_num_workers(2)
    with pytest.raises(RuntimeError, match='does not support concurrent partition writes, cannot be used with Ray'):
        feed._validate_io_for_ray()


def test_validate_io_for_ray_skips_dataflows_without_handler(feed: _Feed):
    feed.set_num_workers(2)
    feed.download('A')
    feed._validate_io_for_ray()


def test_prepare_before_run(feed: _Feed, io: ParquetIO):
    feed.download('A', io=io)
    feed._prepare_before_run()
    assert feed.is_running()
    assert feed.dataflows[0].is_sealed()


def test_prepare_before_run_failure_drops_queued_requests(feed: _Feed, tmp_path: Path):
    feed.set_num_workers(2)
    feed.download('A', io=_NoConcurrentWritesIO(base_path=str(tmp_path)))
    with pytest.raises(RuntimeError, match='cannot be used with Ray'):
        feed._prepare_before_run()
    # otherwise every later run would fail again on the same requests
    assert feed._requests == []
    assert feed.dataflows == []
    assert not feed.is_running()


def test_prepare_before_run_while_running_keeps_the_runs_state(feed: _Feed):
    feed.download('A')
    feed._set_running(True)
    with pytest.raises(RuntimeError, match='already running'):
        feed._prepare_before_run()
    # the other run's requests are untouched
    assert len(feed._requests) == 1
    assert feed.is_running()


# --- run batch dataflows ---


def test_run(feed: _Feed, io: ParquetIO):
    dataflows = feed.download('A', 'B', io=io).transform(_add_one).run()

    assert [df.data_model.data_origin for df in dataflows] == ['A', 'B']
    assert all(df.result.success for df in dataflows)
    # the data is written to io, then read back lazily from it
    for dataflow in dataflows:
        data = dataflow.result.data
        assert isinstance(data, pl.LazyFrame)
        assert sorted(data.collect().select('product', 'price').rows()) == [('A', 2.0), ('B', 2.0)]
    # state is torn down after the run, the feed can run again
    assert not feed.is_running()
    assert feed._requests == []
    assert feed.dataflows == []


def test_run_without_io_returns_the_extracted_data(feed: _Feed):
    (dataflow,) = feed.download('A').transform(_add_one).run()
    data = dataflow.result.data
    assert dataflow.result.success
    assert isinstance(data, pl.LazyFrame)
    assert data.collect().rows() == [('A', 2.0)]


def test_run_with_failed_dataflows_warns(feed: _Feed, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger='pfeed.test'):
        dataflows = feed.download('A', 'FAIL', 'EMPTY').run()

    assert [df.result.success for df in dataflows] == [True, False, False]
    assert 'TEST has 2 failed dataflows' in caplog.text


def test_run_without_failures_does_not_warn(feed: _Feed, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger='pfeed.test'):
        feed.download('A').run()
    assert 'failed dataflows' not in caplog.text


def test_run_cleans_up_when_a_dataflow_raises(feed: _Feed, mocker: MockerFixture):
    mocker.patch.object(DataFlow, 'run_batch', side_effect=KeyboardInterrupt)
    feed.download('A')
    with pytest.raises(KeyboardInterrupt):
        feed.run()
    assert not feed.is_running()
    assert feed._requests == []


@pytest.mark.parametrize(('use_prefect', 'flow_type'), [(False, FlowType.native), (True, FlowType.prefect)])
def test_run_passes_flow_type_and_prefect_kwargs(
    feed: _Feed, mocker: MockerFixture, use_prefect: bool, flow_type: FlowType
):
    run_batch = mocker.patch.object(DataFlow, 'run_batch', autospec=True)
    feed.download('A', 'B')
    dataflows = feed.dataflows

    feed.run(use_prefect=use_prefect, retries=1)

    assert run_batch.call_args_list == [
        mocker.call(dataflow, flow_type=flow_type, prefect_kwargs={'retries': 1}) for dataflow in dataflows
    ]


def test_run_with_prefect_not_installed(feed: _Feed, mocker: MockerFixture):
    from importlib.util import find_spec

    mocker.patch(
        'importlib.util.find_spec',
        side_effect=lambda name, *args: None if name == 'prefect' else find_spec(name, *args),
    )
    feed.download('A')
    with pytest.raises(ImportError, match='use_prefect=True but `prefect` is not installed'):
        feed.run(use_prefect=True)
    # raised before the run started, the queued request is kept
    assert not feed.is_running()
    assert len(feed._requests) == 1


def test_run_with_ray(feed: _Feed, io: ParquetIO, caplog: pytest.LogCaptureFixture):
    pytest.importorskip('ray')
    # more dataflows than workers, so the sliding window submits new tasks as old ones finish
    origins = ['A', 'FAIL', 'B', 'C', 'D']
    feed.set_num_workers(2)
    with caplog.at_level(logging.WARNING, logger='pfeed.test'):
        dataflows = feed.download(*origins, io=io).run()

    # results come back in submission order, whatever order the workers finish in
    assert [df.data_model.data_origin for df in dataflows] == origins
    assert [df.result.success for df in dataflows] == [True, False, True, True, True]
    # the workers wrote to io, every successful dataflow reads it back
    for dataflow in dataflows:
        if dataflow.result.success:
            data = dataflow.result.data
            assert isinstance(data, pl.LazyFrame)
            assert sorted(data.collect()['product'].to_list()) == ['A', 'B', 'C', 'D']
    assert 'TEST has 1 failed dataflows' in caplog.text
    assert not feed.is_running()
