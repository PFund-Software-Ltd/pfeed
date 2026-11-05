from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Hashable

    from pfund_blocks.typing import Message
    from ray.util.queue import Queue

    from pfeed.base.feed import BaseFeed
    from pfeed.base.requests.stream_request import BaseStreamRequest
    from pfeed.base.time_based_data_model import TimeBasedDataModel
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import WorkerName
    from pfeed.dataflow.result import RunResult
    from pfeed.engine import DataEngine
    from pfeed.streaming.zeromq import ZeroMQChannel

    # NOTE: only for the type checker, so that it knows the mixin's self is a feed; at runtime the mixin inherits an empty class.
    # the type arguments are Any because a specific request type conflicts with the one the feed passes to MarketFeed,
    # e.g. BybitMarketFeed uses MarketFeedBaseRequest
    _FeedBase = BaseFeed[Any, Any, Any, Any]
else:
    # NOTE: not object, a generic class can't list object as a base, its MRO with Generic is inconsistent
    class _FeedBase: ...

import asyncio
import contextlib
import inspect
import time

from pfund_blocks.enums.env import Environment


class StreamingMixin[RequestT: BaseStreamRequest, DataModelT: TimeBasedDataModel](_FeedBase):
    QUEUE_MAXSIZE: ClassVar[int] = 1000
    WORKER_READY_TIMEOUT: ClassVar[float] = 60  # seconds
    WORKER_STOP_TIMEOUT: ClassVar[float] = 60  # seconds

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        # NOTE: one queue per dataflow
        self._queues: dict[Hashable, asyncio.Queue[Any]] = {}
        self._user_callback: Callable[[Message], Awaitable[None] | None] | None = None
        # NOTE: only set while iterating the feed with __aiter__()
        self._iter_queue: asyncio.Queue[Message] | None = None
        self._engine: DataEngine | None = None

    def _set_engine(self, engine: DataEngine) -> None:
        self._engine = engine

    async def __aiter__(self) -> AsyncGenerator[Message]:
        """Run the streams and yield each message the user callback gets, e.g. `async for msg in feed.stream(...)` in pipeline mode.

        Breaking out of the loop ends the streams only when the generator is closed, which Python does lazily,
        wrap it in `contextlib.aclosing()` to end them right at the break.
        """
        if not self.is_streaming():
            raise RuntimeError("No streaming dataflow to iterate over")
        queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=self.QUEUE_MAXSIZE)
        self._iter_queue = queue
        producer = asyncio.create_task(self.run_async())
        # NOTE: a non-immediate shutdown lets the consumer drain the queued messages before get() raises QueueShutDown
        producer.add_done_callback(lambda _: queue.shutdown())
        try:
            while True:
                try:
                    msg = await queue.get()
                except asyncio.QueueShutDown:
                    break
                yield msg
            # re-raises the producer's exception, if any
            await producer
        finally:
            # NOTE: the consumer stopped early, e.g. break or cancelled, end the streams
            if not producer.done():
                _ = producer.cancel()
                # NOTE: wait() instead of awaiting the producer, so the producer's cancellation isn't raised here, but this task's own is
                _ = await asyncio.wait([producer])
            self._iter_queue = None

    @staticmethod
    def _create_queue_key_from_request(request: RequestT) -> Hashable:
        """The key of the request's queue, one queue per dataflow.

        _callback() puts each message in the queue with the same key built from the message,
        so the two must match, e.g. (product name, data resolution) for market data.
        """
        raise NotImplementedError

    @staticmethod
    def _create_queue_key_from_message(message: Message) -> Hashable:
        """The key of the queue the message is put in, see _create_queue_key_from_request().

        A message for no dataflow, e.g. an error message without a product, gets a key no queue has, so it is not queued.
        """
        raise NotImplementedError

    def _create_queue(self, request: RequestT) -> None:
        key = self._create_queue_key_from_request(request)
        if key in self._queues:
            raise ValueError(f"{key} is already being streamed")
        self._queues[key] = asyncio.Queue(maxsize=self.QUEUE_MAXSIZE)

    async def _callback(self, message: Message):
        if self._user_callback:
            result = self._user_callback(message)
            if inspect.isawaitable(result):
                await result
        if (iter_queue := self._iter_queue) is not None:
            # NOTE: drop the oldest message instead of blocking, a slow consumer would otherwise stall the websocket reader shared with the dataflows
            if iter_queue.full():
                self.logger.warning(
                    f"{self.name} iteration queue is full, dropping the oldest message - consider increasing QUEUE_MAXSIZE "
                    + f"(current: {self.QUEUE_MAXSIZE}) or improving the consumer's speed"
                )
                _ = iter_queue.get_nowait()
            iter_queue.put_nowait(message)
        # NOTE: error messages can have no product/resolution, so they have no queue
        if (queue := self._queues.get(self._create_queue_key_from_message(message))) is not None:
            # NOTE: the queue is shut down when its dataflow has failed, drop the message, otherwise every new message raises here
            with contextlib.suppress(asyncio.QueueShutDown):
                await queue.put(message)

    def _set_user_callback(self, callback: Callable[[Message], Awaitable[None] | None]):
        # NOTE: a connection has only one callback, a different one would silently replace the existing one
        if self._user_callback and self._user_callback != callback:
            import os

            from pfeed.utils import is_lambda

            def _describe(func: Callable[..., Any]) -> str:
                # NOTE: e.g. a functools.partial or a callable object has no __code__
                if (code := getattr(func, "__code__", None)) is None:
                    return repr(func)
                return f"{getattr(func, '__qualname__', repr(func))} ({os.path.basename(code.co_filename)}:{code.co_firstlineno})"

            msg = (
                "a feed has one callback for all its streams, but this stream() got a different one\n"
                + f"  existing: {_describe(self._user_callback)}\n"
                + f"  new:      {_describe(callback)}"
            )
            if is_lambda(callback) or is_lambda(self._user_callback):
                msg += (
                    "\nUse a named function (def) instead of a lambda, "
                    + "a lambda is a new function every time it is created, even with the same code"
                )
            raise ValueError(msg)
        self._user_callback = callback

    def _create_stream_dataflow(
        self,
        *,
        extract_func: Callable[..., Any],
        data_model: DataModelT,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
    ) -> DataFlow[RequestT, DataModelT]:
        from pfund_kit.style import RichColor, TextStyle

        request = cast("RequestT", self._get_current_request())
        existing_dataflow = self.dataflows[0] if self.dataflows else None
        existing_faucet = existing_dataflow.faucet if existing_dataflow else None
        existing_request = cast("RequestT", existing_faucet.request) if existing_faucet else None
        if existing_request and request.env != existing_request.env:
            raise ValueError(f"cannot stream {request.env} and {existing_request.env} in the same feed")
        self.logger.debug(f"{request.name}:\n{request}\n", style=TextStyle.BOLD + RichColor.GREEN)

        if callback:
            self._set_user_callback(callback)

        if request.env in (Environment.PAPER, Environment.LIVE):
            stream_api = self.data_source.get_stream_api(request.env)
            stream_api.add_channel(request)
            # NOTE: with Ray, the workers parse the messages
            stream_api.set_callback(self._callback, skip_parse_message=self._is_using_ray())
        # NOTE: in replay (BACKTEST), the feed's _replay_impl() calls _callback() itself, there is no stream API
        self._create_queue(request)

        faucet = existing_faucet or self._create_faucet(request=request, extract_func=extract_func)
        default_transformations = self._get_default_transformations(request)
        dataflow = self._create_dataflow(faucet=faucet, data_model=data_model, default_transformations=default_transformations)
        self._dataflows[request] = [dataflow]
        return dataflow

    async def _run_stream_dataflows(self) -> list[DataFlow[RequestT, DataModelT]]:
        from pfeed.config import get_config
        from pfeed.streaming.lake_optimizer import LakeOptimizer

        self._prepare_before_run()
        optimizer = LakeOptimizer(
            [dataflow.handler.io for dataflow in self.dataflows if dataflow.handler is not None],
            interval=get_config().stream_optimize_interval,
            logger=self.logger,
        )

        async def _run_dataflow(dataflow: DataFlow[RequestT, DataModelT], queue: asyncio.Queue[Message]):
            async for message in dataflow.run_stream(queue=queue):
                # NOTE: without the data engine, the message is already stored, nothing else to do.
                # the data engine handles its own failures, see DataEngine._emit()
                if self._engine:
                    await self._engine._forward_dataflow_message(dataflow, message)
            # NOTE: run_stream() records a failure instead of raising.
            # nothing reads this queue anymore, if it fills up, it would block the websocket reader shared with other dataflows
            if not dataflow.result.success:
                queue.shutdown(immediate=True)

        async def _run_dataflows():
            runs = [
                _run_dataflow(dataflow, self._queues[self._create_queue_key_from_request(request)])
                for request, dataflows in self._dataflows.items()
                for dataflow in dataflows
            ]
            optimizing = asyncio.create_task(optimizer.run())
            try:
                # NOTE: watches must come after runs, wait_until_disconnected() returns right away if connect() hasn't been called,
                # gather() starts them in order, and a dataflow's run_stream() reaches connect() before its first wait
                await asyncio.gather(*runs, return_exceptions=True)
            except asyncio.CancelledError:
                self.logger.warning(f"{self.name} dataflows were cancelled, ending streams...")
            finally:
                _ = optimizing.cancel()
                _ = await asyncio.wait([optimizing])

        try:
            if self._is_using_ray():
                import ray
                import zmq
                from pfund_kit.utils.ray import (
                    ray_logging_context,
                    setup_logger_in_ray_task,
                    setup_ray,
                )

                from pfeed.dataflow.result import DataFlowResult
                from pfeed.streaming.zeromq import (
                    ZeroMQ,
                    ZeroMQDataChannel,
                    ZeroMQSignal,
                )

                setup_ray()

                @ray.remote
                def ray_task(
                    logger_name: str,
                    log_queue: Queue,
                    worker_name: WorkerName,
                    dataflows: dict[ZeroMQChannel, DataFlow[RequestT, DataModelT]],
                    router_ports: list[int],
                    engine_ports: list[int],
                ) -> None:
                    logger = setup_logger_in_ray_task(logger_name, log_queue)
                    msg_queue = ZeroMQ(
                        name=f"{logger_name}.stream.{worker_name}",
                        logger=logger,
                        # NOTE: pushes the transformed data to the data engine's PULL, not needed without the engine
                        sender_type=zmq.PUSH if engine_ports else None,
                        sender_method="connect",
                        receiver_type=zmq.DEALER,
                    )
                    # NOTE: e.g. a replayed market data message already holds Bars, decoded back into Bars instead of dicts
                    for channel, dataflow in dataflows.items():
                        data_model = dataflow.data_model
                        msg_queue.register_codec(channel, data_model.StreamMessage, data_model.enc_hook, data_model.dec_hook)
                    try:
                        for port in engine_ports:
                            msg_queue.connect(msg_queue.sender, port)
                        # NOTE: send() drops messages until PUSH has connected, so wait for it before sending the probe below,
                        # then the main process's wait for the probes covers this connection too; if it never connects, the main process times out and kills this worker
                        if engine_ports:
                            msg_queue.sender.poll(flags=zmq.POLLOUT)
                        # NOTE: must be set before connect(); the identity is the name the ROUTER sends to,
                        # the probe tells the ROUTER this worker is routable, the main process waits for it before streaming
                        msg_queue.receiver.setsockopt(zmq.IDENTITY, worker_name.encode())
                        msg_queue.receiver.setsockopt(zmq.PROBE_ROUTER, 1)
                        for port in router_ports:
                            msg_queue.connect(msg_queue.receiver, port)
                        while True:
                            msg = msg_queue.recv()
                            if msg is None:
                                continue
                            channel, topic, data, _msg_ts = msg
                            if channel == ZeroMQDataChannel.signal:
                                if data == ZeroMQSignal.STOP:
                                    logger.debug(f"Ray {worker_name} received STOP signal, terminating...")
                                    break
                            else:
                                # NOTE: the same transformations and storing as in the main process, see DataFlow._run_stream_etl()
                                data = dataflows[channel]._process_stream_message(data)
                                if data is not None and engine_ports:
                                    # NOTE: the engine decodes the data points back into Bar/Tick, see DataEngine._setup_worker_messaging()
                                    msg_queue.send(channel=channel, topic=topic, data=data)
                    except Exception:
                        logger.exception(f"Error in streaming Ray {worker_name}:")
                        raise
                    finally:
                        # NOTE: each worker has its own copies of the handlers holding their own buffered rows, write them before exiting, otherwise the rows are lost;
                        # a failed write raises, so ray.get() in the main process marks the worker's dataflows as failed
                        try:
                            for dataflow in dataflows.values():
                                dataflow._teardown_handler()
                        finally:
                            msg_queue.terminate()

                assert self._num_workers is not None, "num_workers is not set"
                dataflows = self.dataflows
                # NOTE: each stream worker holds a CPU for the whole stream, workers beyond Ray's free CPUs would never start
                num_cpus = max(1, int(ray.available_resources().get("CPU", 1)))
                num_workers = min(self._num_workers, len(dataflows), num_cpus)
                # NOTE: a dataflow stays on one worker, so its messages stay in order and its stateful transformations stay in one process
                dataflows_per_worker: dict[WorkerName, list[DataFlow[RequestT, DataModelT]]] = {
                    f"worker-{i + 1}": dataflows[i::num_workers] for i in range(num_workers)
                }

                with ray_logging_context(self.logger) as log_queue:
                    futures: list[ray.ObjectRef] = []
                    try:
                        for worker_name, worker_dataflows in dataflows_per_worker.items():
                            for dataflow in worker_dataflows:
                                dataflow.faucet.add_stream_worker(dataflow, worker_name)
                        # NOTE: all dataflows share one faucet, so there is only one ROUTER to connect to
                        msg_queue = dataflows[0].faucet.msg_queue
                        router = msg_queue.sender
                        router_ports = msg_queue.get_ports_in_use(router)
                        # NOTE: stays empty when the feed runs without the data engine, workers then have nothing to push to
                        engine_ports: list[int] = []
                        if self._engine:
                            engine_ports = self._engine._get_worker_ports()
                        # NOTE: the keys are the dataflows' zmq channels, every message from Faucet.send_to_stream_worker() carries its channel
                        futures = [
                            ray_task.remote(
                                self.logger.name,
                                log_queue,
                                worker_name,
                                # NOTE: pickled without their faucet, see DataFlow.__getstate__()
                                {dataflow.faucet.get_zmq_channel(dataflow): dataflow for dataflow in worker_dataflows},
                                router_ports,
                                engine_ports,
                            )
                            for worker_name, worker_dataflows in dataflows_per_worker.items()
                        ]
                        # NOTE: a ROUTER can't send to a worker until the handshake is done, even after the worker's connect() has returned,
                        # so wait for the probe each worker sends on connecting
                        missing_workers = set(dataflows_per_worker)
                        deadline = time.monotonic() + self.WORKER_READY_TIMEOUT
                        while missing_workers:
                            remaining_ms = int((deadline - time.monotonic()) * 1000)
                            if remaining_ms <= 0 or not router.poll(remaining_ms):
                                raise TimeoutError(f"{msg_queue.sender_name} timed out waiting for workers: {sorted(missing_workers)}")
                            identity, *_ = router.recv_multipart()
                            missing_workers.discard(identity.decode())
                        await _run_dataflows()
                    finally:
                        # NOTE: end_stream() must run for every dataflow for proper teardown, it records its own errors, so one bad teardown doesn't skip the rest
                        await asyncio.gather(*[dataflow.end_stream() for dataflow in self.dataflows])
                        # NOTE: end_stream() has sent STOP, wait for the workers to flush their handlers and exit before killing them
                        finished, _ = ray.wait(futures, num_returns=len(futures), timeout=self.WORKER_STOP_TIMEOUT) if futures else ([], [])
                        for future, (worker_name, worker_dataflows) in zip(futures, dataflows_per_worker.items(), strict=False):
                            try:
                                if future in finished:
                                    # NOTE: if the worker function raised, ray.get() re-raises its exception here; returns None if it exited normally
                                    ray.get(future)
                                else:
                                    raise TimeoutError(f"Ray {worker_name} did not stop within {self.WORKER_STOP_TIMEOUT}s, its buffered rows may be lost")
                            except Exception as err:
                                for dataflow in worker_dataflows:
                                    dataflow._result = DataFlowResult.failed(error=err)
                            # NOTE: force=True kills the worker, a non-forced cancel doesn't stop a task that's already running
                            ray.cancel(future, force=True)
            else:
                try:
                    await _run_dataflows()
                finally:
                    # NOTE: end_stream() must run for every dataflow for proper teardown, it records its own errors, so one bad teardown doesn't skip the rest
                    await asyncio.gather(*[dataflow.end_stream() for dataflow in self.dataflows])

            if failed := [
                dataflow
                for dataflow in self.dataflows
                if not dataflow.result.success
            ]:
                self.logger.warning(
                    f"{self.name} has {len(failed)} failed dataflows, check {self.logger.name}.log for more details",
                )

            return self.dataflows
        finally:
            # NOTE: after every handler (incl. the Ray workers' copies) has flushed, so the last rows are optimized too
            optimizer.optimize()
            self._queues.clear()
            self._user_callback = None
            self._cleanup_after_run()

    def run(self, use_prefect: bool = False, **prefect_kwargs: Any) -> RunResult:
        if not self.is_streaming():
            return super().run(use_prefect=use_prefect, **prefect_kwargs)
        try:
            _ = asyncio.get_running_loop()
        except RuntimeError:
            # no running event loop, safe to use asyncio.run()
            pass
        else:
            raise RuntimeError(
                "Cannot call feed.run() from within a running event loop.\n"
                + "Did you mean to call feed.run_async() or forget to set 'pipeline_mode=True'?"
            )
        return asyncio.run(self.run_async())

    async def run_async(self) -> RunResult:
        from pfeed.dataflow.result import RunResult

        if not self.is_streaming():
            raise RuntimeError(
                f"{self.name} run_async() is only for streaming dataflows. Use run() instead."
            )
        dataflows = await self._run_stream_dataflows()
        return RunResult(data=None, dataflows=dataflows)
