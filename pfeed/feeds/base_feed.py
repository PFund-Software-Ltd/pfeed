# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportAttributeAccessIssue=false, reportUnusedParameter=false, reportMissingTypeArgument=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportArgumentType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Callable, ClassVar, Any, cast
if TYPE_CHECKING:
    from prefect import Flow as PrefectFlow
    from ray.util.queue import Queue
    from pfeed.sources.base_source import BaseSource
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.requests.base_request import BaseRequest
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.storages.storage_config import StorageConfig
    from pfeed._io.io_config import IOConfig
    from pfeed.dataflow.result import DataFlowResult

import os
import logging
from collections import defaultdict
from abc import ABC, abstractmethod

from pfeed.enums import ExtractType, DataLayer, FlowType, DataCategory


__all__ = []


class BaseFeed(ABC):
    data_model_class: ClassVar[type[BaseDataModel]]
    data_domain: ClassVar[DataCategory]

    def __init__(self, pipeline_mode: bool=False, num_workers: int | None = None):
        '''
        Args:
            pipeline_mode: whether to run in pipeline mode
            num_workers: number of Ray tasks to run the batch/streaming dataflows in parallel.
                When provided, Ray will be automatically initialized if ray.init() hasn't been called yet.
                To customize Ray initialization (e.g. specifying num_cpus), call ray.init() explicitly before using this.
        '''
        from pfeed.config import setup_logging
        setup_logging()
        self.data_source: BaseSource = self._create_data_source()
        self.logger: logging.Logger = logging.getLogger(f'pfeed.{self.name.lower()}')
        self._pipeline_mode = pipeline_mode
        self._dataflows: dict[BaseRequest, list[DataFlow]] = {}
        # Flat list of result-bearing dataflows from the most recent run.
        # Survives `_cleanup_after_run` clearing `_dataflows`, so post-run callers
        # can access `completed_dataflows` / `failed_dataflows` regardless of
        # whether Ray was used (Ray returns new objects from workers).
        self._last_run_dataflows: list[DataFlow] = []
        self._requests: list[BaseRequest] = []
        self._custom_transformations: dict[BaseRequest, list[Callable[..., Any]]] = defaultdict(list)
        self._num_workers: int | None = num_workers
        self._is_running = False
        if self._num_workers:
            self.set_num_workers(self._num_workers)

    @staticmethod
    @abstractmethod
    def _create_data_source() -> BaseSource:
        pass

    @abstractmethod
    def _create_data_model_from_request(self, request: BaseRequest) -> BaseDataModel:
        pass

    @abstractmethod
    def create_data_model(self, *args: Any, **kwargs: Any) -> BaseDataModel:
        pass

    @abstractmethod
    def _create_batch_dataflows(self, *args: Any, **kwargs: Any):
        pass

    @abstractmethod
    def run(self, **prefect_kwargs: Any) -> Any:
        pass

    def is_pipeline(self) -> bool:
        return self._pipeline_mode

    def is_running(self) -> bool:
        return self._is_running

    def _is_using_ray(self) -> bool:
        return bool(self._num_workers)

    @property
    def name(self):
        return self.data_source.name

    @property
    def dataflows(self) -> list[DataFlow]:
        # Before/during a run: read from the build-up dict. After a run (when
        # `_dataflows` has been cleared by `_cleanup_after_run`): fall back to the
        # captured `_last_run_dataflows` so post-run inspection keeps working.
        if self._dataflows:
            return [df for dfs in self._dataflows.values() for df in dfs]
        return list(self._last_run_dataflows)

    @property
    def streaming_dataflows(self) -> list[DataFlow]:
        return [dataflow for dataflow in self.dataflows if dataflow.is_streaming()]

    @property
    def completed_dataflows(self) -> list[DataFlow]:
        """Dataflows from the most recent run whose result.success is True."""
        return [df for df in self._last_run_dataflows if df.result.success]

    @property
    def failed_dataflows(self) -> list[DataFlow]:
        """Dataflows from the most recent run whose result.success is False."""
        return [df for df in self._last_run_dataflows if not df.result.success]

    def to_prefect_dataflows(self, **kwargs: Any) -> list[PrefectFlow]:
        '''
        Args:
            kwargs: kwargs specific to prefect @flow decorator

        Note: this is an export, not a run — once the prefect flows leave this
        feed, the user owns their lifecycle. We only validate; we deliberately
        skip `_prepare_before_run` / `_cleanup_after_run` so we don't leave the
        feed in an `is_running=True` state with no way to clear it.
        '''
        self._validate_before_run()
        return [dataflow.to_prefect_dataflow(**kwargs) for dataflow in self.dataflows]

    def supports_streaming(self) -> bool:
        from pfeed.feeds.streaming_feed_mixin import StreamingFeedMixin
        return isinstance(self, StreamingFeedMixin)

    def set_num_workers(self, num_workers: int):
        '''
        Sets the number of workers for parallel execution using Ray.
        The number of workers is capped by the available CPU count.

        Args:
            num_workers: The desired number of workers. Must be greater than 0.
        '''
        assert num_workers > 0, 'num_workers must be greater than 0'
        num_cpus: int = cast(int, os.cpu_count())
        self._num_workers = min(num_workers, num_cpus)
        from pfeed.utils.ray import setup_ray
        setup_ray()

    def _set_running(self, is_running: bool) -> None:
        if self.is_running() and is_running:
            raise RuntimeError(f'{self} is already running')
        self._is_running = is_running

    @staticmethod
    def _create_dataflow(faucet: Faucet, data_model: BaseDataModel) -> DataFlow:
        from pfeed.dataflow.dataflow import DataFlow
        assert faucet.data_source == data_model.data_source, 'faucet and data_model must have the same data source'
        dataflow = DataFlow(faucet=faucet, data_model=data_model)
        return dataflow

    @staticmethod
    def _create_faucet(
        data_source: BaseSource,
        extract_func: Callable[..., Any],
        extract_type: ExtractType,
    ) -> Faucet:
        '''
        Args:
            data_source: the data source to extract from
            extract_func: the function to perform the extraction, e.g. _download_impl(), _stream_impl(), _retrieve_impl()
            extract_type: the type of the extraction, e.g. download, stream, retrieve
        '''
        from pfeed.dataflow.faucet import Faucet
        return Faucet(
            data_source=data_source,
            extract_func=extract_func,
            extract_type=extract_type,
        )

    def _get_current_request(self) -> BaseRequest:
        request = self._requests[-1] if self._requests else None
        if request is None:
            raise AssertionError('No current request found, did you forget to call download()/retrieve()/stream() first?')
        return request

    def transform(self, *funcs: Callable[..., Any]) -> BaseFeed:
        request = self._get_current_request()
        self._custom_transformations[request].extend(funcs)
        return self

    def load(self, storage_config: StorageConfig | None = None, io_config: IOConfig | None = None) -> BaseFeed:
        from pfeed._io.io_config import IOConfig
        request = self._check_current_request('load')
        # allowing passing in None is useful for dynamically determining if load() is needed
        if storage_config is None:
            return self
        io_config = io_config or IOConfig()
        Storage = storage_config.storage.storage_class
        for dataflow in self._dataflows[request]:
            storage = cast(
                "BaseStorage", (
                    Storage
                    .from_storage_config(storage_config)
                    .with_io(io_config)
                    .with_data_model(dataflow.data_model)
                )
            )
            if request.is_streaming():
                storage.data_handler.create_stream_buffer(
                    mode=request.mode,
                    flush_interval=request.flush_interval,
                )
            dataflow.set_storage(storage)
        return self

    def _validate_before_run(self) -> None:
        for request, dataflows in self._dataflows.items():
            custom_transformations = self._custom_transformations[request]
            for dataflow in dataflows:
                storage = dataflow.storage
                if storage and storage.data_layer == DataLayer.RAW and custom_transformations:
                    raise RuntimeError(
                        'Custom transformations are not allowed when data layer is RAW'
                    )

    def _prepare_before_run(self):
        self._last_run_dataflows = []
        self._validate_before_run()
        self._set_running(True)

    def _cleanup_after_run(self):
        self._requests.clear()
        self._dataflows = {}
        self._custom_transformations = defaultdict(list)
        self._set_running(False)

    def _run_batch_dataflows(self, prefect_kwargs: dict[str, Any]) -> list[DataFlow]:
        from pfund_kit.utils.progress_bar import track, ProgressBar
        from pfeed.utils import is_prefect_running

        use_prefect = is_prefect_running()
        self._prepare_before_run()

        def _run_dataflow(dataflow: DataFlow) -> DataFlowResult:
            return dataflow.run_batch(
                flow_type=FlowType.prefect if use_prefect else FlowType.native,
                prefect_kwargs=prefect_kwargs
            )

        # Collect the result-bearing dataflow objects here. For non-Ray these are the
        # same objects as in `self._dataflows` (mutated in place); for Ray they're the
        # deserialized copies returned from workers (which carry the actual results).
        post_run: list[DataFlow] = []

        try:
            if self._is_using_ray():
                import ray
                from pfeed.utils.ray import setup_logger_in_ray_task, ray_logging_context

                @ray.remote
                def ray_task(logger_name: str, dataflow: DataFlow, log_queue: Queue) -> DataFlow:
                    logger = setup_logger_in_ray_task(logger_name, log_queue)
                    try:
                        _ = _run_dataflow(dataflow)
                    except RuntimeError as err:
                        if use_prefect:
                            logger.exception(f'Error in running prefect {dataflow}:')
                            dataflow.mark_failed(err)
                        else:
                            raise
                    except Exception as err:
                        logger.exception(f'Error in running {dataflow}:')
                        dataflow.mark_failed(err)
                    return dataflow

                with ray_logging_context(self.logger) as log_queue:
                    try:
                        assert self._num_workers is not None, 'num_workers is not set'
                        batch_size = self._num_workers
                        dataflow_batches = [self.dataflows[i: i + batch_size] for i in range(0, len(self.dataflows), batch_size)]
                        with ProgressBar(total=len(self.dataflows), description=f'Running {self.name} dataflows') as pbar:
                            for dataflow_batch in dataflow_batches:
                                futures = [
                                    ray_task.remote(
                                        logger_name=self.logger.name,  # pyright: ignore[reportCallIssue]
                                        dataflow=dataflow,
                                        log_queue=log_queue,
                                    ) for dataflow in dataflow_batch
                                ]
                                unfinished = futures
                                while unfinished:
                                    finished, unfinished = ray.wait(unfinished, num_returns=1)
                                    for future in finished:
                                        dataflow = ray.get(future)
                                        post_run.append(dataflow)
                                        pbar.advance(1)
                    except KeyboardInterrupt:
                        self.logger.warning(f"KeyboardInterrupt received, stopping {self.name} dataflows...")
                    except Exception:
                        self.logger.exception(f'Error in running {self.name} dataflows:')
                self.logger.debug('ray tasks finished')
                # NOTE: Do NOT shut down Ray here to avoid interfering with Ray's control in higher-level frameworks that might also be using Ray.
                # shutdown_ray()
            else:
                for dataflow in track(self.dataflows, description=f'Running {self.name} dataflows'):
                    try:
                        _ = _run_dataflow(dataflow)
                    except RuntimeError as err:
                        if use_prefect:
                            self.logger.exception(f'Error in running prefect {dataflow}:')
                            dataflow.mark_failed(err)
                        else:
                            raise
                    except Exception as err:
                        self.logger.exception(f'Error in running {dataflow}:')
                        dataflow.mark_failed(err)
                    post_run.append(dataflow)

            if failed := [dataflow for dataflow in post_run if not dataflow.result.success]:
                only_retrieval_dataflows = all(dataflow.extract_type == ExtractType.retrieve for dataflow in failed)
                log_level = logging.DEBUG if only_retrieval_dataflows else logging.WARNING
                self.logger.log(
                    log_level,
                    f'{self.name} has {len(failed)} failed dataflows, check {self.logger.name}.log for more details'
                )

            return post_run
        finally:
            # Always commit partial progress and tear down state, even if the run aborted.
            # Without this, an escaping exception (e.g. the non-prefect RuntimeError re-raise)
            # would leave `_is_running=True`, locking the feed against any future `.run()`.
            self._last_run_dataflows = post_run
            self._cleanup_after_run()
