# pyright: reportMissingTypeArgument=false, reportUnknownParameterType=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportAttributeAccessIssue=false, reportAssignmentType=false, reportArgumentType=false
from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame
    from prefect import Flow as PrefectDataFlow

    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.dataflow.faucet import Faucet
    from pfeed.feeds.streaming_feed_mixin import RawMessage, StreamingData
    from pfeed.sources.base_source import BaseSource
    from pfeed.storages.base_storage import BaseStorage
    from pfeed.streaming.zeromq import ZeroMQ

import logging

import polars as pl
from pfund.enums.data_channel import PublicDataChannel

from pfeed.dataflow.result import DataFlowResult
from pfeed.enums import ExtractType, FlowType


class DataFlow:
    def __init__(self, faucet: Faucet, data_model: BaseDataModel):
        self._data_model: BaseDataModel = data_model
        self._logger = logging.getLogger(f"pfeed.{self.data_source.name.lower()}")
        self._faucet: Faucet = faucet
        self._transformations: list[Callable[..., IntoFrame | StreamingData]] = []
        self._storage: BaseStorage | None = None
        self._result = DataFlowResult()
        self._flow_type: FlowType = FlowType.native
        self._zmq_channel: str = ""
        self._zmq_topic: PublicDataChannel | None = None
        self._assigned_stream_worker: str | None = None
        self._is_sealed = False

    def _setup_messaging(self):
        from pfeed.data_models.market_data_model import MarketDataModel
        from pfeed.streaming.zeromq import ZeroMQDataChannel

        data_model: MarketDataModel = cast(MarketDataModel, self._data_model)
        self._zmq_channel: str = ZeroMQDataChannel.create_market_data_channel(
            # data_source=self.data_source.name,
            product=data_model.product,
            resolution=data_model.resolution,
        )
        if data_model.resolution.is_quote():
            self._zmq_topic = PublicDataChannel.orderbook
        elif data_model.resolution.is_tick():
            self._zmq_topic = PublicDataChannel.tradebook
        else:
            self._zmq_topic = PublicDataChannel.candlestick

    def set_stream_worker(self, worker_name: str):
        if self._assigned_stream_worker is None:
            self._setup_messaging()
        self._assigned_stream_worker = worker_name
        self.faucet.add_stream_worker(worker_name)

    @property
    def name(self):
        return f"{self.data_source.name}_DataFlow"

    @property
    def data_source(self) -> BaseSource:
        return self._data_model.data_source

    @property
    def data_model(self) -> BaseDataModel:
        return self._data_model

    @property
    def faucet(self) -> Faucet:
        return self._faucet

    @property
    def _msg_queue(self) -> ZeroMQ | None:
        return self.faucet._msg_queue

    @property
    def storage(self) -> BaseStorage | None:
        return self._storage

    @property
    def extract_type(self) -> ExtractType:
        return self.faucet.extract_type

    @property
    def result(self) -> DataFlowResult:
        return self._result

    def add_default_transformations(self, funcs: list[Callable[..., Any]]):
        if self.is_sealed():
            raise ValueError(
                f"cannot add default transformations to sealed dataflow {self.name}"
            )
        self._transformations = funcs + self._transformations

    def add_user_transformations(self, funcs: list[Callable[..., Any]]):
        if self.is_sealed():
            raise ValueError(
                f"cannot add transformations to sealed dataflow {self.name}"
            )
        self._transformations.extend(funcs)

    def has_storage(self) -> bool:
        return self._storage is not None

    def set_storage(self, storage: BaseStorage):
        if self._storage is not None:
            raise ValueError(f"storage is already set for dataflow {self.name}")
        if self.is_sealed():
            raise ValueError(f"cannot set storage for sealed dataflow {self.name}")
        self._storage = storage

    def __str__(self):
        if not self.is_streaming():
            return f"{self.name}.{self.extract_type}"
        else:
            return f"{self.name}.{self.extract_type}.{self.data_model}"

    def is_streaming(self) -> bool:
        return self.extract_type == ExtractType.stream

    def is_replaying(self) -> bool:
        # In BACKTEST the streaming dataflow's storage is the source it READS replay
        # data from, not a write destination — so it must never write back.
        from pfund.enums.env import Environment

        return (
            self.is_streaming()
            and getattr(self._data_model, "env", None) == Environment.BACKTEST
        )

    def is_sealed(self) -> bool:
        return self._is_sealed

    def seal(self):
        self._is_sealed = True

    def _run_batch_etl(self) -> pl.LazyFrame | None:
        from pfeed.utils.dataframe import is_dataframe, is_empty_dataframe

        if self._flow_type == FlowType.prefect:
            from prefect import task

            extract = task(self.faucet.open_batch)
        else:
            extract = self.faucet.open_batch
        data: pl.LazyFrame | None = extract(data_model=self.data_model)
        if (data is not None) and not (is_dataframe(data) and is_empty_dataframe(data)):
            data = self._transform(data)
            self._load(data)
        return data

    def run_batch(
        self, flow_type: FlowType = FlowType.native, prefect_kwargs: dict | None = None
    ) -> DataFlowResult:
        if not self.is_sealed():
            raise ValueError(f"cannot run batch on unsealed dataflow {self.name}")
        self._logger.debug(
            f"{self} to storage={self.storage.data_path if self.storage else None}"
        )

        self._flow_type = FlowType[flow_type.lower()]
        if self._flow_type == FlowType.prefect:
            prefect_dataflow = self.to_prefect_dataflow(**(prefect_kwargs or {}))
            data = prefect_dataflow()
        else:
            data = self._run_batch_etl()

        if data is None:
            self._result = DataFlowResult.failed(
                error=ValueError(f"{self.name} produced no data")
            )
        elif self.storage is not None:
            # Lazy-load from storage on access so Ray workers don't copy large frames back to main.
            self._result = DataFlowResult.lazy(loader=self.storage.read)
        else:
            self._result = DataFlowResult.materialized(data=data)
        return self._result

    def mark_failed(self, error: BaseException) -> None:
        """Replace the dataflow's result with a failed one carrying the error.

        Called from the batch runner's catch sites so the failure reason survives
        beyond a log line and is accessible via `dataflow.result.error`.
        """
        self._result = DataFlowResult.failed(error=error)

    def _run_stream_etl(self, msg: RawMessage) -> None:
        # Replay is read-only: the raw row already reached the user callback and the
        # streaming queue in the faucet. Its storage is the READ source, so there is
        # nothing to transform or load here — skip the whole ETL-to-storage step.
        if self.is_replaying():
            return
        # NOTE: if zeromq is in use (when using ray), send msg to Ray's worker and let it perform ETL
        if self._msg_queue:
            self._msg_queue.send(
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
            f"{self} to storage={self.storage.data_path if self.storage else None}"
        )
        self._flow_type = FlowType[flow_type.lower()]
        await self.faucet.open_stream(
            data_model=self.data_model, storage=self.storage
        )  # this will trigger _run_stream_etl()

    async def end_stream(self):
        await self.faucet.close_stream()

    def _transform(self, data: IntoFrame | StreamingData) -> IntoFrame | StreamingData:
        for transform in self._transformations:
            if self.is_streaming():
                data: StreamingData = transform(data)
            else:
                if self._flow_type == FlowType.prefect:
                    from prefect import task
                    from prefect.utilities.annotations import quote

                    transform = task(transform)
                    # NOTE: Removing prefect's task introspection with `quote(data)` to save time
                    data: IntoFrame = transform(quote(data))
                else:
                    data: IntoFrame = transform(data)
            assert data is not None, (
                f"transform function {transform} should return transformed data, but got None"
            )
            self._logger.debug(
                f"transformed {self.data_model} data by '{transform.__name__}'"
            )
        return data

    def _load(self, data: IntoFrame | StreamingData):
        if not self.has_storage():
            return
        try:
            if self.is_streaming():
                self._storage.write(data, streaming=True)
            else:
                if self._flow_type == FlowType.prefect:
                    from prefect import task

                    write = task(self._storage.write)
                else:
                    write = self._storage.write
                write(data)
            self._logger.debug(f"loaded {self.data_model} data to {self._storage}")
        except Exception:
            self._logger.exception(
                f"failed to load {self.data_model} data to {self._storage}:"
            )

    def to_prefect_dataflow(self, **kwargs: Any) -> PrefectDataFlow:
        """
        Converts dataflow to prefect flow
        Args:
            kwargs: kwargs specific to prefect @flow decorator
        """
        from prefect import flow

        if "log_prints" not in kwargs:
            kwargs["log_prints"] = True

        @flow(name=self.name, flow_run_name=str(self.data_model), **kwargs)
        def prefect_flow():
            # from prefect.logging import get_run_logger
            # prefect_logger = get_run_logger()  # this is a logger adapter
            self._flow_type = FlowType.prefect
            return self._run_batch_etl()

        return prefect_flow
