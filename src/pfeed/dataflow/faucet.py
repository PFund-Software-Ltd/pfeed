from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Concatenate, ParamSpec, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from pfeed.base.data_model import BaseDataModel
    from pfeed.base.request import BaseRequest
    from pfeed.source import BaseSource
    from pfeed.streaming.zeromq import ZeroMQ

    ExtractFuncParams = ParamSpec("ExtractFuncParams")


import asyncio
import logging
import random
import time

import polars as pl


class Faucet[RequestT: BaseRequest, DataModelT: BaseDataModel]:
    """Faucet is the starting point of a dataflow
    It contains a data model and a flow function to perform the extraction.
    """

    STREAMING_QUEUE_MAXSIZE: ClassVar[int] = 1000

    def __init__(
        self,
        request: RequestT,
        extract_func: Callable[
            Concatenate[RequestT, DataModelT, ExtractFuncParams], Any
        ],  # e.g. _download_impl(), _stream_impl(), _retrieve_impl()
    ):
        self._logger = logging.getLogger(f"pfeed.{request.data_source.name.lower()}")
        self._data_source: BaseSource = request.data_source
        self._request = request
        self._extract_func = extract_func
        self._stream_task: asyncio.Task[None] | None = None
        self._msg_queue: ZeroMQ | None = None
        self._stream_workers: list[str] = []

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

    def _setup_messaging(self):
        import zmq

        from pfeed.streaming.zeromq import ZeroMQ

        assert self._logger is not None, "logger is not set"
        self._msg_queue = ZeroMQ(
            name="Faucet",
            logger=self._logger,
            sender_type=zmq.ROUTER,
        )
        self._msg_queue.bind(self._msg_queue.sender)

    def add_stream_worker(self, worker_name: str):
        if not self._stream_workers:
            self._setup_messaging()
        if worker_name not in self._stream_workers:
            self._stream_workers.append(worker_name)

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
                return cast(
                    pl.LazyFrame | None,
                    self._extract_func(self._request, data_model),
                )
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

    def open_stream(self, data_model: DataModelT) -> asyncio.Task[None]:
        # NOTE: streaming dataflows share the same faucet, so we only need to start the extraction once
        if self._stream_task is None:
            self._stream_task = asyncio.create_task(self._extract_func(self._request, data_model))
        return self._stream_task

    async def close_stream(self):
        if self._stream_task is not None:
            # NOTE: all dataflows call this, swap first so that only the first one cancels
            stream_task, self._stream_task = self._stream_task, None
            # _stream_impl() disconnects in its finally
            stream_task.cancel()
            await asyncio.wait({stream_task})
        if self._msg_queue:
            self._msg_queue.terminate(target_identities=self._stream_workers)
            self._msg_queue = None
            self._stream_workers.clear()
