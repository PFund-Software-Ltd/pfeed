from __future__ import annotations

from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from pfund_blocks.typing import Message

    from pfeed.base.data_model import BaseDataModel
    from pfeed.base.requests.base_request import BaseRequest
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.source import BaseSource
    from pfeed.streaming.zeromq import ZeroMQ, ZeroMQChannel, ZeroMQTopic

    ExtractFuncParams = ParamSpec("ExtractFuncParams")
    type WorkerName = str

import asyncio
import logging
import random
import time

import polars as pl


class Faucet[RequestT: BaseRequest, DataModelT: BaseDataModel]:
    def __init__(
        self,
        request: RequestT,
        # NOTE: a batch extract fetches each dataflow's data model, e.g. _download_impl(), _retrieve_impl();
        # a stream extract runs once for all dataflows, so it takes no data model, e.g. _stream_impl(), _replay_impl()
        extract_func: Callable[Concatenate[RequestT, DataModelT, ExtractFuncParams], Any]
        | Callable[[RequestT], Coroutine[Any, Any, None]],
    ):
        self._logger = logging.getLogger(f"pfeed.{request.data_source.name.lower()}")
        self._data_source: BaseSource = request.data_source
        self._request = request  # NOTE: for streaming/replaying, this is always the first request
        self._extract_func = extract_func
        self._stream_task: asyncio.Task[None] | None = None
        self._msg_queue: ZeroMQ | None = None
        self._stream_workers: dict[DataFlow[RequestT, DataModelT], WorkerName] = {}
        self._zmq_routes: dict[DataFlow[RequestT, DataModelT], tuple[ZeroMQChannel, ZeroMQTopic]] = {}

    @property
    def name(self) -> str:
        return f"{self._data_source.name}.{self.extract_type}"

    @property
    def extract_type(self) -> str:
        return self._request.extract_type

    @property
    def request(self) -> RequestT:
        return self._request

    @property
    def msg_queue(self) -> ZeroMQ:
        assert self._msg_queue is not None, "msg_queue is not set"
        return self._msg_queue

    def open_batch(self, data_model: DataModelT) -> pl.LazyFrame | None:
        policy = self._request.retry_policy
        attempt = 0
        while True:
            # TODO: rate limiting, e.g. source.rate_limiter.acquire() here before every attempt (retries included).
            # - the limit (calls per period) comes from the source's APIAccess, the limiter is owned by the source
            #   so all its feeds/faucets share one budget
            # - this only counts extract calls; a source making many requests per extract should acquire per request
            # - under Ray each worker gets its own limiter copy, so split the budget by num_workers (or use a Ray actor)
            try:
                extract_func = cast("Callable[[RequestT, DataModelT], pl.LazyFrame | None]", self._extract_func)
                return extract_func(self._request, data_model)
            except policy.retry_on as e:
                if attempt == policy.max_retries:
                    e.add_note(f'{self.name} extraction of {data_model} failed, gave up after {policy.max_retries} retries')
                    raise
                wait = policy.wait * policy.backoff**attempt * random.uniform(0.5, 1.5)
                attempt += 1
                self._logger.warning(
                    f'{self.name} extraction of {data_model} failed ({e!r}), '
                    f'retry {attempt}/{policy.max_retries} in {wait:.1f}s'
                )
                time.sleep(wait)

    def open_stream(self) -> asyncio.Task[None]:
        # NOTE: streaming dataflows share the same faucet, so we only need to start the extraction once
        if self._stream_task is None:
            extract_func = cast("Callable[[RequestT], Coroutine[Any, Any, None]]", self._extract_func)
            self._stream_task = asyncio.create_task(extract_func(self._request))
        return self._stream_task

    def _register_zmq_route(self, dataflow: DataFlow[RequestT, DataModelT]):
        import zmq

        from pfeed.streaming.zeromq import ZeroMQ

        if self._msg_queue is None:
            self._msg_queue = ZeroMQ(name="Faucet", logger=self._logger, sender_type=zmq.ROUTER)
            self._msg_queue.bind(self._msg_queue.sender)
        data_model = dataflow.data_model
        self._zmq_routes[dataflow] = route = data_model.to_zmq_route()
        # NOTE: e.g. a replayed market data message holds Bars
        self._msg_queue.register_codec(route[0], data_model.StreamMessage, data_model.enc_hook, data_model.dec_hook)

    def add_stream_worker(self, dataflow: DataFlow[RequestT, DataModelT], worker_name: WorkerName):
        self._stream_workers[dataflow] = worker_name
        self._register_zmq_route(dataflow)

    def get_zmq_channel(self, dataflow: DataFlow[RequestT, DataModelT]) -> ZeroMQChannel:
        return self._zmq_routes[dataflow][0]

    def is_using_ray(self) -> bool:
        """Checks if using ray for streaming"""
        return bool(self._stream_workers)

    def send_to_stream_worker(self, dataflow: DataFlow[RequestT, DataModelT], msg: Message):
        channel, topic = self._zmq_routes[dataflow]
        worker_name: WorkerName = self._stream_workers[dataflow]
        self.msg_queue.send(
            channel=channel,
            topic=topic,
            data=msg,
            target_identity=worker_name,
        )

    async def close_stream(self):
        if self._stream_task is not None:
            # NOTE: all dataflows call this, swap first so that only the first one cancels
            stream_task, self._stream_task = self._stream_task, None
            # _stream_impl() disconnects in its finally
            stream_task.cancel()
            await asyncio.wait({stream_task})
        if self._msg_queue:
            # workers can serve several dataflows, each only needs one STOP
            self._msg_queue.terminate(target_identities=list(set(self._stream_workers.values())))
            self._msg_queue = None
            self._stream_workers.clear()
            self._zmq_routes.clear()
