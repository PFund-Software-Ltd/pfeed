from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from narwhals.typing import IntoFrame
    from pfund_blocks.typing import Message
    from prefect import Flow as PrefectDataFlow

    from pfeed.base.data_handler import BaseDataHandler
    from pfeed.base.data_model import BaseDataModel
    from pfeed.base.requests import BaseRequest, BaseStreamRequest
    from pfeed.dataflow.faucet import Faucet
    from pfeed.enums import DataLayer
    from pfeed.io.base_io import BaseIO

import asyncio
import logging

import polars as pl

from pfeed.dataflow.result import DataFlowResult
from pfeed.enums import FlowType


class DataFlow[RequestT: BaseRequest, DataModelT: BaseDataModel]:
    def __init__(
        self,
        faucet: Faucet[RequestT, DataModelT],
        data_model: DataModelT,
        default_transformations: list[Callable[..., Any]] | None = None,
    ):
        """
        Args:
            default_transformations: the feed's transformations, run before the user's from transform()
        """
        self._logger = logging.getLogger(f"pfeed.{data_model.data_source.lower()}")
        self._faucet: Faucet[RequestT, DataModelT] | None = faucet
        # NOTE: kept on the dataflow, not read from the faucet, since a stream worker's copy has no faucet, see __getstate__()
        self._extract_type: str = faucet.extract_type
        self._is_streaming: bool = faucet.request.is_streaming()
        self._is_replaying: bool = self._is_streaming and faucet.request.is_replaying()
        self._data_model: DataModelT = data_model
        self._default_transformations: list[Callable[..., Any]] = list(default_transformations or [])
        self._user_transformations: list[Callable[..., Any]] = []
        self._handler: BaseDataHandler | None = None
        self._result = DataFlowResult()
        self._flow_type: FlowType = FlowType.native
        self._is_sealed = False

    def __getstate__(self) -> dict[str, Any]:
        # NOTE: a stream's faucet holds the live stream (e.g. its zmq ROUTER, extract task and stream API), which can't be pickled,
        # and a Ray stream worker doesn't need it, it gets the messages from the faucet's ROUTER;
        # a batch worker runs the whole dataflow, extract included, so it keeps its faucet
        state = self.__dict__.copy()
        if self._is_streaming:
            state["_faucet"] = None
        return state

    @property
    def data_model(self) -> DataModelT:
        return self._data_model

    @property
    def faucet(self) -> Faucet[RequestT, DataModelT]:
        if self._faucet is None:
            raise RuntimeError(f"{self} has no faucet, e.g. a copy in a Ray stream worker")
        return self._faucet

    @property
    def handler(self) -> BaseDataHandler | None:
        return self._handler

    @property
    def result(self) -> DataFlowResult:
        return self._result

    def __str__(self):
        if self.is_streaming():
            return f"{self._extract_type}:{self.data_model}"
        return f"{self.data_model.data_source}.{self._extract_type}"

    def is_streaming(self) -> bool:
        return self._is_streaming

    def is_replaying(self) -> bool:
        return self._is_replaying

    def is_sealed(self) -> bool:
        return self._is_sealed

    def seal(self):
        self._is_sealed = True

    def add_user_transformations(self, funcs: list[Callable[..., Any]]):
        if self.is_sealed():
            raise ValueError(
                f"cannot add transformations to sealed dataflow {self}"
            )
        self._user_transformations.extend(funcs)

    def _transform(self, data: IntoFrame | Message, transformations: list[Callable[..., Any]] | None = None) -> Any:
        """Runs the transformations on data, all of them (default then user) if None."""
        if transformations is None:
            transformations = self._default_transformations + self._user_transformations
        for transform in transformations:
            if self.is_streaming():
                data = transform(data)
            else:
                if self._flow_type == FlowType.prefect:
                    from prefect import task
                    from prefect.cache_policies import NO_CACHE
                    from prefect.utilities.annotations import quote

                    # NO_CACHE: the default cache policy hashes the input data on every call
                    transform = task(cache_policy=NO_CACHE)(transform)
                    # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                    data: IntoFrame = cast("IntoFrame", transform(quote(data)))
                else:
                    data: IntoFrame = cast("IntoFrame", transform(data))
            func_name = getattr(transform, "__name__", repr(transform))
            if data is None:
                raise TypeError(f"transform function '{func_name}' should return transformed data, but got None")
            self._logger.debug(f"transformed {self.data_model} data by '{func_name}'")
        return data

    def has_handler(self) -> bool:
        return self._handler is not None

    def setup_handler(self, io: BaseIO, data_layer: DataLayer, data_domain: str, **handler_kwargs: Any):
        """Creates the data handler that loads the data to io, from the dataflow's own data model.

        Args:
            handler_kwargs: options specific to the data model's handler, forwarded as is,
                e.g. store_incremental_bars for MarketDataHandler.
        """
        if self.has_handler():
            raise ValueError(f"handler is already set for dataflow {self}")
        if self.is_sealed():
            raise ValueError(f"cannot set handler for sealed dataflow {self}")
        is_storing_stream = self.is_streaming() and not self.is_replaying()
        self._handler = self._data_model.DataHandler(
            data_model=self._data_model,
            io=io,
            data_layer=data_layer,
            data_domain=data_domain,
            is_streaming=is_storing_stream,
            **handler_kwargs,
        )
        if is_storing_stream:
            request = cast("BaseStreamRequest", self.faucet.request)
            self._handler.setup_streaming(request.flush_interval)

    def _load(self, data: Any) -> bool:
        """Returns whether the data was loaded, False if the dataflow has no handler."""
        if self._handler is None:
            return False
        try:
            if self.is_streaming():
                self._handler.write_stream(data)
            else:
                from pfeed.utils.dataframe import convert_dataframe

                # user transformations may return any dataframe, the data handler only takes polars
                df = cast(pl.LazyFrame, convert_dataframe(data)).collect()
                if self._flow_type == FlowType.prefect:
                    from prefect import task
                    from prefect.cache_policies import NO_CACHE

                    write = task(name=self._handler.name, cache_policy=NO_CACHE)(self._handler.write_batch)
                else:
                    write = self._handler.write_batch
                write(df)
            self._logger.debug(f"loaded {self.data_model} data to {self._handler}")
            return True
        except Exception:
            # Re-raise so the batch runner marks the dataflow failed; a write
            # error must surface in RunResult, not only in the log.
            self._logger.exception(f"failed to load {self.data_model} data to {self._handler}:")
            raise

    def _run_batch_etl(self) -> DataFlowResult:
        """Runs extract, transform and load, sets and returns the dataflow's result.

        Empty data is also loaded, so its partitions are stored as empty and not re-downloaded,
        but it is not read back, since an empty partition reads back as None.
        """
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe

        if self._flow_type == FlowType.prefect:
            from prefect import task
            from prefect.cache_policies import NO_CACHE

            extract = task(name=self.faucet.name, cache_policy=NO_CACHE)(self.faucet.open_batch)
        else:
            extract = self.faucet.open_batch

        extracted_data: pl.LazyFrame | None = extract(data_model=self.data_model)
        if extracted_data is None:
            self._logger.debug(f"no {self.data_model} data extracted by {self.faucet.name}")
            # retrieve reads storage, where missing data is a valid answer, the feed reports the missing dates
            if self.faucet.extract_type == "retrieve":
                self._result = DataFlowResult.no_data()
            else:
                self._result = DataFlowResult.failed(error=ValueError(f"{self} produced no data"))
            return self._result
        self._logger.debug(f"extracted {self.data_model} data by {self.faucet.name}")
        is_empty = is_dataframe(extracted_data) and is_empty_dataframe(extracted_data)
        data = cast("IntoFrame", self._transform(extracted_data))
        is_loaded = self._load(data)
        # an empty partition reads back as None, so keep the empty frame itself as the result
        if is_loaded and not is_empty and (handler := self.handler) is not None:
            # Lazy-load from storage on access so Ray workers don't copy large frames back to main.
            # read() also returns the metadata, the result only needs the data
            self._result = DataFlowResult.lazy(loader=lambda: handler.read()[0])
        else:
            self._result = DataFlowResult.materialized(data=data)
        return self._result

    def run_batch(self, flow_type: FlowType = FlowType.native, prefect_kwargs: dict | None = None) -> DataFlowResult:
        if not self.is_sealed():
            raise ValueError(f"cannot run batch on unsealed dataflow {self}")

        self._logger.debug(f"{self} to {self.handler!r}" if self.handler else f"{self} (no storage)")
        self._flow_type = FlowType[flow_type.lower()]

        # a failed run is recorded as the dataflow's result, so one failure doesn't stop the other dataflows
        try:
            if self._flow_type == FlowType.prefect:
                prefect_dataflow = self.to_prefect_dataflow(**(prefect_kwargs or {}))
                return prefect_dataflow()
            else:
                return self._run_batch_etl()
        except Exception as err:
            self._logger.exception(f"Error in running {self}:")
            self._result = DataFlowResult.failed(error=err)
            return self._result

    def to_prefect_dataflow(self, **kwargs: Any) -> PrefectDataFlow:
        """
        Converts dataflow to prefect flow
        Args:
            kwargs: kwargs specific to prefect @flow decorator
        """
        from prefect import flow

        kwargs.setdefault("log_prints", True)  # captures print() in user transformations
        kwargs.setdefault("name", str(self))
        kwargs.setdefault("flow_run_name", str(self.data_model))

        @flow(**kwargs)
        def prefect_flow() -> DataFlowResult:
            # from prefect.logging import get_run_logger
            # prefect_logger = get_run_logger()  # this is a logger adapter
            self._flow_type = FlowType.prefect
            # send the data source's logs (e.g. "pfeed.bybit") to the prefect UI, like PREFECT_LOGGING_EXTRA_LOGGERS,
            # which prefect only applies at import time; "pfeed.<source>" doesn't propagate, so attach to it directly.
            # Only during the run, outside a flow run prefect's API handler warns on every log
            added_handlers = [h for h in logging.getLogger("prefect.extra").handlers if h not in self._logger.handlers]
            for handler in added_handlers:
                self._logger.addHandler(handler)
            try:
                return self._run_batch_etl()
            finally:
                for handler in added_handlers:
                    self._logger.removeHandler(handler)

        return prefect_flow

    def _process_stream_message(self, msg: Message) -> Any | None:
        """Transforms and stores one streamed message, in the main process or in a Ray stream worker.

        Returns:
            The message transformed by the user's transformations, None if a transformation failed.
        """
        # NOTE: stores the message as transformed by the feed (e.g. parsed, resampled), before the user's transformations,
        # so the stored stream stays pfeed's standardized data whatever the user's transformations return
        try:
            data = self._transform(msg, self._default_transformations)
        # e.g. a transformation can't handle this message
        except Exception:
            self._logger.exception(f"{self} transformation failed, skipping message: {msg}")
            return None
        self._load(data)
        try:
            return self._transform(data, self._user_transformations)
        except Exception:
            self._logger.exception(f"{self} user transformation failed, skipping message: {data}")
            return None

    async def _run_stream_etl(self, queue: asyncio.Queue[Message]) -> None:
        # Replay is read-only: the raw row already reached the user callback and the
        # streaming queue in the faucet. Its storage is the READ source, so there is
        # nothing to transform or load here — skip the whole ETL-to-storage step.
        if self.is_replaying():
            return
        extract = self.faucet.open_stream(self.data_model)
        try:
            while True:
                msg = await queue.get()
                if self.faucet.is_using_ray():
                    self.faucet.send_to_stream_worker(self, msg)
                    continue
                _ = self._process_stream_message(msg)
        except asyncio.QueueShutDown:
            pass
        await extract

    async def run_stream(self, queue: asyncio.Queue[Message], flow_type: FlowType = FlowType.native) -> None:
        if not self.is_sealed():
            raise ValueError(f"cannot run stream on unsealed dataflow {self}")

        self._logger.info(f"{self} to {self.handler!r}" if self.handler else f"{self} (no storage)")
        self._flow_type = FlowType[flow_type.lower()]
        # a stream has no final data, its result only reports whether it failed,
        # so it stays successful unless an error below overwrites it
        self._result = DataFlowResult.no_data()
        # a failed stream is recorded as the dataflow's result, so one failure doesn't stop the other dataflows;
        # cancellation (graceful shutdown) is not an Exception, it propagates and is not a failure
        try:
            return await self._run_stream_etl(queue)
        except Exception as err:
            self._logger.exception(f"Error in running {self}:")
            self._result = DataFlowResult.failed(error=err)

    async def end_stream(self):
        try:
            await self.faucet.close_stream()
            # with Ray, each worker flushes its own copy of the handler, this one's buffer stays empty
            if self.has_handler():
                self._handler.flush()
        except Exception as err:
            self._logger.exception(f"Error ending stream for {self}:")
            self._result = DataFlowResult.failed(error=err)
