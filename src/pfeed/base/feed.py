from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from pfund_kit.logging.loggers import ColoredLogger
    from ray.util.queue import Queue

    from pfeed.base.data_model import BaseDataModel
    from pfeed.base.request import BaseRequest
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.result import DataFlowResult
    from pfeed.io.base_io import BaseIO
    from pfeed.source import BaseSource

import functools
import logging
from abc import ABC, abstractmethod

from pfeed.enums import DataCategory, DataLayer, FlowType


def requires[F: Callable[..., Any]](verb: str) -> Callable[[F], F]:
    """Gates a public verb method (e.g. download) on the feed's Requests, before any of its body runs."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(self: BaseFeed[Any, Any, Any], *args: Any, **kwargs: Any) -> Any:
            if not self._supports(verb):
                raise NotImplementedError(f"{self.name} does not support {verb} for {self.data_domain}")
            return func(self, *args, **kwargs)

        return cast("F", wrapper)

    return decorator


class BaseFeed[SourceT: BaseSource, RequestT: BaseRequest, DataModelT: BaseDataModel](ABC):
    # set by the source that lists this feed in its Feeds, see BaseSource.__init_subclass__
    DataSource: ClassVar[type[BaseSource]]
    DataModel: ClassVar[type[BaseDataModel]]

    class Requests:
        """Request class per verb. Each feed family subclasses this with its own verbs, None = not supported."""

    # methods a feed must override for each verb it supports
    _required_methods: ClassVar[dict[str, tuple[str, ...]]] = {}
    data_domain: ClassVar[DataCategory]

    def __init_subclass__(cls, **kwargs: Any):
        from pfeed.streaming.feed_mixin import StreamingFeedMixin

        super().__init_subclass__(**kwargs)
        # framework base classes (listing ABC directly) hold the defaults, so only concrete feeds are checked
        if ABC in cls.__bases__:
            return
        errors: list[str] = []
        for verb in cls.capabilities():
            missing: list[str] = []
            for method in cls._required_methods.get(verb, ()):
                owner = next((c for c in cls.__mro__ if method in c.__dict__), None)
                # not defined at all, or still the default from a framework base class
                if owner is None or ABC in owner.__bases__:
                    missing.append(method)
            if verb == "stream" and not issubclass(cls, StreamingFeedMixin):
                missing.insert(0, "StreamingFeedMixin as a base class")
            # downloaded data must be readable back, retrieve needs nothing from the feed, so leaving it out is a mistake
            if verb == "download" and hasattr(cls.Requests, "retrieve") and "retrieve" not in cls.capabilities():
                missing.insert(0, "Requests.retrieve")
            if missing:
                errors.append(f"'{verb}' needs {', '.join(missing)}")
        if errors:
            raise TypeError(
                f"{cls.__name__} supports {cls.capabilities()} but is missing what they need:\n  "
                + "\n  ".join(errors)
                + f"\nImplement them, or set the verbs to None in {cls.__name__}.Requests"
            )

    def __init__(self, data_source: SourceT, pipeline_mode: bool = False, num_workers: int | None = None):
        """
        Args:
            data_source: the source that owns this feed, shared by all of its feeds
            pipeline_mode: whether to run in pipeline mode
            num_workers: number of Ray tasks to run the batch/streaming dataflows in parallel.
                When provided, Ray will be automatically initialized if ray.init() hasn't been called yet.
                To customize Ray initialization (e.g. specifying num_cpus), call ray.init() explicitly before using this.
        """
        from pfeed.config import setup_logging

        setup_logging()
        if not isinstance(data_source, self.DataSource):
            raise TypeError(
                f"{type(self).__name__} belongs to {self.DataSource.__name__}, "
                f"got {type(data_source).__name__}"
            )
        self._data_source: SourceT = data_source
        self.logger: ColoredLogger = cast("ColoredLogger", logging.getLogger(f"pfeed.{self.name.lower()}"))
        self._pipeline_mode = pipeline_mode
        self._dataflows: dict[RequestT, list[DataFlow]] = {}
        self._requests: list[RequestT] = []
        self._num_workers: int | None = None
        self._is_running = False
        self.set_num_workers(num_workers)

    @classmethod
    @abstractmethod
    def create_data_model(cls, *args: Any, **kwargs: Any) -> BaseDataModel:
        pass

    @abstractmethod
    def _get_default_transformations(self, request: RequestT, /) -> list[Callable[..., Any]]:
        pass

    @abstractmethod
    def run(self, use_prefect: bool = False, **prefect_kwargs: Any) -> Any:
        """
        Args:
            use_prefect: whether to run the batch dataflows as Prefect flows, requires the `prefect` extra.
            prefect_kwargs: kwargs specific to prefect @flow decorator, only used if use_prefect is True.
        """

    def is_pipeline(self) -> bool:
        return self._pipeline_mode

    def is_running(self) -> bool:
        return self._is_running

    def _is_using_ray(self) -> bool:
        return bool(self._num_workers)

    @classmethod
    def capabilities(cls) -> list[str]:
        """Verbs this feed supports, i.e. the Requests attributes not set to None."""
        return sorted(
            verb for verb in dir(cls.Requests)
            if not verb.startswith("_") and getattr(cls.Requests, verb) is not None
        )

    def _supports(self, verb: str) -> bool:
        return verb in self.capabilities()

    @property
    def data_source(self) -> SourceT:
        return self._data_source

    @property
    def name(self):
        return self.data_source.name

    @property
    def dataflows(self) -> list[DataFlow]:
        return [df for dfs in self._dataflows.values() for df in dfs]

    @property
    def streaming_dataflows(self) -> list[DataFlow]:
        return [dataflow for dataflow in self.dataflows if dataflow.is_streaming()]

    def set_num_workers(self, num_workers: int | None) -> None:
        """
        Sets the number of Ray workers to run the dataflows in parallel, None to run without Ray.

        Args:
            num_workers: The desired number of workers. Must be greater than 0.
                Not capped by the local CPU count, Ray may run on a cluster and queues the tasks it can't schedule yet.
        """
        if self.is_running():
            raise RuntimeError(f"cannot set num_workers while {self.name} is running")
        if num_workers is not None and num_workers <= 0:
            raise ValueError("num_workers must be greater than 0")
        self._num_workers = num_workers

    def _set_running(self, is_running: bool) -> None:
        if self.is_running() and is_running:
            raise RuntimeError(f"{self} is already running")
        self._is_running = is_running

    @staticmethod
    def _create_dataflow(
        faucet: Faucet, data_model: BaseDataModel, default_transformations: list[Callable[..., Any]]
    ) -> DataFlow:
        from pfeed.dataflow.dataflow import DataFlow

        dataflow = DataFlow(faucet=faucet, data_model=data_model, default_transformations=default_transformations)
        return dataflow

    def _create_faucet(self, request: RequestT, extract_func: Callable[..., Any]) -> Faucet:
        """
        Args:
            request: the request the faucet extracts data for
            extract_func: the function to perform the extraction, e.g. _download_impl(), _stream_impl(), _retrieve_impl()
        """
        from pfeed.dataflow.faucet import Faucet

        return Faucet(request=request, extract_func=extract_func)

    def _get_current_request(self) -> RequestT:
        request = self._requests[-1] if self._requests else None
        if request is None:
            raise AssertionError(
                "No current request found, did you forget to call e.g. download()/retrieve()/stream() first?"
            )
        return request

    def _append_request(self, request: RequestT) -> None:
        # In pipeline mode requests accumulate before run(), but run() dispatches a
        # single path based on whether streaming dataflows exist — batch and streaming
        # requests cannot be mixed in one feed run. Reject the mix at the source rather
        # than letting a batch faucet hit run_stream() (or vice versa) downstream.
        if (
            self._requests
            and request.is_streaming() != self._requests[-1].is_streaming()
        ):
            mode, queued_mode = ("streaming", "batch") if request.is_streaming() else ("batch", "streaming")
            raise ValueError(
                "cannot mix streaming and batch requests in one pipeline: "
                + f"{mode} request is incompatible with the queued {queued_mode} requests"
            )
        self._requests.append(request)

    def transform(self, *funcs: Callable[..., Any]) -> Self:
        request = self._get_current_request()
        for dataflow in self._dataflows[request]:
            dataflow.add_user_transformations(list(funcs))
        return self

    def load(self, io: BaseIO | None = None, data_layer: DataLayer | str | None = None) -> Self:
        """Store the data of the request just created.

        Args:
            io: Where and how to store the data. If None, the request's io; if that is also None, data is not stored.
            data_layer: Data layer to store the data in. If None, the request's data layer.
                CLEANED data can be stored as CURATED (e.g. after transform()),
                other combinations must match the request's data layer.
        """
        # fluent-chain entry point: always targets the request just created
        current_request = self._get_current_request()
        return self._load_for_request(request=current_request, io=io, data_layer=data_layer)

    def _load_for_request(
        self,
        request: RequestT,
        io: BaseIO | None = None,
        data_layer: DataLayer | str | None = None,
    ) -> Self:
        io = io or request.io
        # no io anywhere, nothing to store
        if io is None:
            return self
        data_layer = request.data_layer if data_layer is None else DataLayer[str(data_layer).upper()]
        request.finalize_load_config(io, data_layer)
        for dataflow in self._dataflows[request]:
            dataflow.setup_handler(io, data_layer, str(self.data_domain))
        return self

    def _finalize_run(self) -> None:
        # run-time method: finalize EVERY queued request, not just the latest.
        # In pipeline mode multiple download()/retrieve()/stream() calls accumulate
        # requests before run(), and `dataflows` runs all of them — so each must be
        # sealed. (Unlike transform()/load(), which target the current request only.)
        for request in self._requests:
            dataflows = self._dataflows[request]
            # load() was not called, store the data in the request's io (if any)
            if not any(dataflow.has_handler() for dataflow in dataflows):
                _ = self._load_for_request(request=request)
            for dataflow in dataflows:
                dataflow.seal()

    def _validate_io_for_ray(self) -> None:
        """Checks that every io the dataflows write to supports concurrent writes from Ray workers.

        Checked at run time, so num_workers can be set before or after load().
        """
        if not self._is_using_ray():
            return
        for dataflow in self.dataflows:
            if (handler := dataflow.handler) is not None and not handler.io.CAPABILITIES.concurrent_partition_writes:
                raise RuntimeError(
                    f"{handler.io} does not support concurrent partition writes, cannot be used with Ray"
                )

    def _prepare_before_run(self):
        # raises if already running, before touching the other run's state
        self._set_running(True)
        try:
            self._finalize_run()
            self._validate_io_for_ray()
        except Exception:
            # drop the queued requests, otherwise every later run re-finalizes them and fails again
            self._cleanup_after_run()
            raise

    def _cleanup_after_run(self):
        self._requests.clear()
        self._dataflows = {}
        self._set_running(False)

    def _create_batch_dataflows(self, *, extract_func: Callable[..., Any], data_models: list[DataModelT]) -> list[DataFlow]:
        """Creates batch dataflows for the current request, one per data model

        Args:
            extract_func: the function to perform the extraction, e.g. _download_impl(), _retrieve_impl()
            data_models: the data models to create dataflows for.
        """
        from pfund_kit.style import RichColor, TextStyle

        request = self._get_current_request()
        self.logger.debug(f"{request.name}:\n{request}\n", style=TextStyle.BOLD + RichColor.GREEN)
        faucet = self._create_faucet(request=request, extract_func=extract_func)
        default_transformations = self._get_default_transformations(request)
        dataflows = [
            self._create_dataflow(faucet=faucet, data_model=dm, default_transformations=default_transformations)
            for dm in data_models
        ]
        self._dataflows[request] = dataflows
        return dataflows

    def _run_batch_dataflows(self, use_prefect: bool, prefect_kwargs: dict[str, Any]) -> list[DataFlow]:
        from importlib.util import find_spec

        from pfund_kit.utils.progress_bar import ProgressBar, track

        from pfeed.config import get_config

        if use_prefect and find_spec("prefect") is None:
            raise ImportError('use_prefect=True but `prefect` is not installed. Install it with `pip install "pfeed[prefect]"`.')
        disable_progress_bar = not get_config().show_progress_bar
        self._prepare_before_run()

        def _run_dataflow(dataflow: DataFlow) -> DataFlowResult:
            return dataflow.run_batch(
                flow_type=FlowType.prefect if use_prefect else FlowType.native,
                prefect_kwargs=prefect_kwargs,
            )

        # Collect the result-bearing dataflow objects here. For non-Ray these are the
        # same objects as in `self._dataflows` (mutated in place); for Ray they're the
        # deserialized copies returned from workers (which carry the actual results).
        last_run_dataflows: list[DataFlow] = []

        try:
            if self._is_using_ray():
                from functools import partial
                from itertools import islice

                import ray

                from pfund_kit.utils.ray import (
                    ray_logging_context,
                    setup_logger_in_ray_task,
                    setup_ray,
                )

                setup_ray()

                # if feed.run() is called inside a prefect flow, pass its context to the Ray workers,
                # so their dataflows become its subflows, not separate top-level flow runs
                prefect_context: dict[str, Any] | None = None
                if use_prefect:
                    from prefect.context import FlowRunContext, serialize_context

                    if FlowRunContext.get() is not None:
                        prefect_context = serialize_context()

                @ray.remote
                def ray_task(
                    logger_name: str, log_queue: Queue, prefect_context: dict[str, Any] | None, dataflow: DataFlow
                ) -> DataFlow:
                    from contextlib import nullcontext

                    _ = setup_logger_in_ray_task(logger_name, log_queue)
                    if prefect_context is not None:
                        from prefect.context import hydrated_context

                        context = hydrated_context(prefect_context)
                    else:
                        context = nullcontext()
                    with context:
                        _ = _run_dataflow(dataflow)
                    return dataflow

                with ray_logging_context(self.logger) as log_queue:
                    unfinished: list[ray.ObjectRef] = []
                    try:
                        assert self._num_workers is not None, "num_workers is not set"
                        # Ray's .remote() only takes positional arguments (for type checkers), dataflow goes last
                        submit = partial(ray_task.remote, self.logger.name, log_queue, prefect_context)
                        dataflows = self.dataflows
                        pending = iter(enumerate(dataflows))
                        # dataflows finish in any order, keep the results in submission order so run() is deterministic
                        ref_to_index: dict[ray.ObjectRef, int] = {}
                        results: list[DataFlow | None] = [None] * len(dataflows)

                        def submit_next(index: int, dataflow: DataFlow) -> ray.ObjectRef:
                            ref = submit(dataflow)
                            ref_to_index[ref] = index
                            return ref

                        # sliding window: keep num_workers tasks in flight, start the next dataflow
                        # as soon as one finishes, so a slow dataflow doesn't leave the other workers idle
                        unfinished = [submit_next(*item) for item in islice(pending, self._num_workers)]
                        with ProgressBar(
                            total=len(dataflows),
                            description=f"Running {self.name} dataflows",
                            disable=disable_progress_bar,
                        ) as pbar:
                            while unfinished:
                                finished, unfinished = ray.wait(unfinished, num_returns=1)
                                results[ref_to_index[finished[0]]] = ray.get(finished[0])
                                pbar.advance(1)
                                if (item := next(pending, None)) is not None:
                                    unfinished.append(submit_next(*item))
                        last_run_dataflows.extend(cast("list[DataFlow]", results))
                    except BaseException:
                        # e.g. KeyboardInterrupt or a crashed worker: cancel the in-flight tasks,
                        # otherwise they keep running (and writing to storage) after run() returns.
                        # force=True kills the worker, a non-forced cancel doesn't stop a task that's already running
                        for future in unfinished:
                            ray.cancel(future, force=True)
                        raise
                self.logger.debug("ray tasks finished")
                # NOTE: Do NOT shut down Ray here to avoid interfering with Ray's control in higher-level frameworks that might also be using Ray.
                # shutdown_ray()
            else:
                for dataflow in track(
                    self.dataflows,
                    description=f"Running {self.name} dataflows",
                    disable=disable_progress_bar,
                ):
                    _ = _run_dataflow(dataflow)
                    last_run_dataflows.append(dataflow)

            if failed := [
                dataflow
                for dataflow in last_run_dataflows
                if not dataflow.result.success
            ]:
                self.logger.warning(
                    f"{self.name} has {len(failed)} failed dataflows, check {self.logger.name}.log for more details",
                )

            return last_run_dataflows
        finally:
            # Always tear down state, even if the run aborted.
            # Without this, an escaping exception (e.g. KeyboardInterrupt)
            # would leave `_is_running=True`, locking the feed against any future `.run()`.
            self._cleanup_after_run()
