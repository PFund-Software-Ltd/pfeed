from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from narwhals.typing import IntoFrame
    from prefect import Flow as PrefectDataFlow

    from pfeed.base.data_handler import BaseDataHandler
    from pfeed.base.data_model import BaseDataModel
    from pfeed.dataflow.faucet import Faucet
    from pfeed.enums import DataLayer
    from pfeed.io.base_io import BaseIO
    from pfeed.streaming.feed_mixin import RawMessage, StreamingData

import logging

import polars as pl

from pfeed.dataflow.result import DataFlowResult
from pfeed.enums import FlowType


class DataFlow:
    def __init__(
        self,
        faucet: Faucet,
        data_model: BaseDataModel,
        default_transformations: list[Callable[..., Any]] | None = None,
    ):
        """
        Args:
            default_transformations: the feed's transformations, run before the user's from transform()
        """
        self._logger = logging.getLogger(f"pfeed.{data_model.data_source.lower()}")
        self._faucet: Faucet = faucet
        self._data_model: BaseDataModel = data_model
        self._transformations: list[Callable[..., IntoFrame | StreamingData]] = list(default_transformations or [])
        self._handler: BaseDataHandler | None = None
        self._result = DataFlowResult()
        self._flow_type: FlowType = FlowType.native
        self._zmq_channel: str = ""
        self._zmq_topic: str | None = None
        self._assigned_stream_worker: str | None = None
        self._is_sealed = False
        # set by set_stream_options(), only for streaming dataflows
        self._flush_interval: float | None = None
        self._write_stream_kwargs: dict[str, Any] = {}

    @property
    def name(self):
        return f"{self._data_model.data_source}_DataFlow"

    @property
    def data_model(self) -> BaseDataModel:
        return self._data_model

    @property
    def faucet(self) -> Faucet:
        return self._faucet

    @property
    def handler(self) -> BaseDataHandler | None:
        return self._handler

    @property
    def result(self) -> DataFlowResult:
        return self._result

    def __str__(self):
        if not self.is_streaming():
            return f"{self.name}.{self.faucet.extract_type}"
        else:
            return f"{self.name}.{self.faucet.extract_type}.{self.data_model}"

    def is_streaming(self) -> bool:
        return self.faucet.extract_type == "stream"

    def is_replaying(self) -> bool:
        # In BACKTEST the streaming dataflow's storage is the source it READS replay
        # data from, not a write destination — so it must never write back.
        from pfund_blocks.enums.env import Environment

        return (
            self.is_streaming()
            and getattr(self._data_model, "env", None) == Environment.BACKTEST
        )

    def is_sealed(self) -> bool:
        return self._is_sealed

    def seal(self):
        self._is_sealed = True

    def add_user_transformations(self, funcs: list[Callable[..., Any]]):
        if self.is_sealed():
            raise ValueError(
                f"cannot add transformations to sealed dataflow {self.name}"
            )
        self._transformations.extend(funcs)

    def _transform(self, data: IntoFrame | StreamingData) -> IntoFrame | StreamingData:
        for transform in self._transformations:
            if self.is_streaming():
                data: StreamingData = cast("StreamingData", transform(data))
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

    def setup_handler(self, io: BaseIO, data_layer: DataLayer, data_domain: str):
        """Creates the data handler that loads the data to io, from the dataflow's own data model.

        A streaming dataflow's handler buffers the messages in a Sink, except when replaying,
        where the handler only reads the data to replay.
        """
        from pfeed.streaming.sink import Sink

        if self._handler is not None:
            raise ValueError(f"handler is already set for dataflow {self.name}")
        if self.is_sealed():
            raise ValueError(f"cannot set handler for sealed dataflow {self.name}")
        sink = None
        if self.is_streaming() and not self.is_replaying():
            if self._flush_interval is None:
                raise ValueError(f"stream options are not set for dataflow {self.name}, see set_stream_options()")
            sink = Sink(flush_interval=self._flush_interval)
        self._handler = self._data_model.DataHandler(
            data_model=self._data_model,
            io=io,
            data_layer=data_layer,
            data_domain=data_domain,
            sink=sink,
        )

    def _load(self, data: IntoFrame | StreamingData) -> bool:
        """Returns whether the data was loaded, False if the dataflow has no handler."""
        if self._handler is None:
            return False
        try:
            if self.is_streaming():
                self._handler.write_stream(data, **self._write_stream_kwargs)
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
                self._result = DataFlowResult.failed(error=ValueError(f"{self.name} produced no data"))
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
            raise ValueError(f"cannot run batch on unsealed dataflow {self.name}")

        self._logger.debug(f"{self} to {self.handler!r}")
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
        kwargs.setdefault("name", self.name)
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

    def _setup_messaging(self):
        from pfund_blocks.enums.channel import PublicChannel

        from pfeed.market.data_model import MarketDataModel
        from pfeed.streaming.zeromq import ZeroMQDataChannel

        data_model: MarketDataModel = cast(MarketDataModel, self._data_model)
        self._zmq_channel: str = ZeroMQDataChannel.create_market_data_channel(
            # data_source=self._data_model.data_source,
            product=data_model.product,
            resolution=data_model.resolution,
        )
        if data_model.resolution.is_quote():
            self._zmq_topic = PublicChannel.orderbook
        elif data_model.resolution.is_tick():
            self._zmq_topic = PublicChannel.tradebook
        else:
            self._zmq_topic = PublicChannel.candlestick

    def set_stream_worker(self, worker_name: str):
        if self._assigned_stream_worker is None:
            self._setup_messaging()
        self._assigned_stream_worker = worker_name
        self.faucet.add_stream_worker(worker_name)

    def set_stream_options(self, flush_interval: float, **write_stream_kwargs: Any):
        """Sets how streamed data is stored, before setup_handler().

        Args:
            flush_interval: seconds between writes of the buffered streamed data, see Sink.
            write_stream_kwargs: passed to the handler's write_stream() with every message,
                e.g. store_incremental_bars for MarketDataHandler.
        """
        if not self.is_streaming():
            raise ValueError(f"cannot set stream options for non-streaming dataflow {self.name}")
        if self._handler is not None:
            raise ValueError(f"cannot set stream options after the handler is set for dataflow {self.name}")
        self._flush_interval = flush_interval
        self._write_stream_kwargs = write_stream_kwargs

    def _run_stream_etl(self, msg: RawMessage) -> None:
        # Replay is read-only: the raw row already reached the user callback and the
        # streaming queue in the faucet. Its storage is the READ source, so there is
        # nothing to transform or load here — skip the whole ETL-to-storage step.
        if self.is_replaying():
            return
        # NOTE: if zeromq is in use (when using ray), send msg to Ray's worker and let it perform ETL
        if self.faucet.msg_queue:
            self.faucet.msg_queue.send(
                channel=self._zmq_channel,
                topic=self._zmq_topic,
                data=(self.name, msg),
                target_identity=self._assigned_stream_worker,
            )
        else:
            try:
                data = cast("StreamingData", self._transform(msg))
            # e.g. ValueError from StreamingMessage.__post_init__ validation, ValidationError from msgspec.convert
            except Exception:
                self._logger.exception(
                    f"{self.name} failed to transform streaming message:"
                )
                # REVIEW: we will NOT load the bad data to storage, unless it can handle raw msg, for now, just return
                # data = msg
                return
            self._load(data)

    async def run_stream(self, flow_type: Literal["native"] = "native"):
        if not self.is_sealed():
            raise ValueError(f"cannot run stream on unsealed dataflow {self.name}")
        self._logger.info(
            f"{self} to {self.handler!r}"
        )
        self._flow_type = FlowType[flow_type.lower()]
        # a failed stream is recorded as the dataflow's result, so one failure doesn't stop the other dataflows;
        # cancellation (graceful shutdown) is not an Exception, it propagates and is not a failure
        try:
            await self.faucet.open_stream(
                data_model=self.data_model, handler=self.handler
            )  # this will trigger _run_stream_etl()
        except Exception as err:
            self._logger.exception(f"Error in running {self}:")
            self._result = DataFlowResult.failed(error=err)

    async def end_stream(self):
        try:
            await self.faucet.close_stream()
            # with Ray, each worker flushes its own copy of the handler, this one's sink stays empty
            if self._handler is not None and self._handler.sink is not None:
                self._handler.flush()
        except Exception as err:
            self._logger.exception(f"Error ending stream for {self}:")
            self._result = DataFlowResult.failed(error=err)
