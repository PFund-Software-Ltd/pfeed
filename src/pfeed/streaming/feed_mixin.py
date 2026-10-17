from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from pfund.enums.env import Environment

    from pfeed.base.data_handler import BaseDataHandler
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.result import RunResult
    from pfeed.engine import DataEngine
    from pfeed.market.requests.stream_request import MarketFeedStreamRequest
    from pfeed.streaming.streaming_message import StreamingMessage
    from pfeed.streaming.zeromq import ZeroMQ, ZeroMQSignal

    ReplayData: TypeAlias = dict[str, Any]
    WebSocketName: TypeAlias = str
    WorkerName: TypeAlias = str
    DataFlowName: TypeAlias = str
    ChannelKey: TypeAlias = str | tuple[str, str]
    RawMessage: TypeAlias = dict[str, Any]
    StreamingData = RawMessage | StreamingMessage

import asyncio
from abc import ABC
from collections import defaultdict

from pfund_kit.style import RichColor, TextStyle


def _create_worker_name(worker_num: int) -> str:
    return f"worker-{worker_num}"


# EXTEND: only support market feed for now, if need to support other feeds, fix MarketFeedStreamRequest and MarketDataModel
# ABC marks it as a framework base, so BaseFeed treats its default methods as not implemented
class StreamingFeedMixin(ABC):  # noqa: B024
    _engine: DataEngine | None = None

    def _get_stream_api(self, env: Environment) -> Any:
        """Returns the stream API this feed streams from, e.g. one of its data source's APIs."""
        raise NotImplementedError

    def _set_engine(self, engine: DataEngine) -> None:
        self._engine = engine

    def __aiter__(
        self,
    ) -> AsyncGenerator[
        tuple[WebSocketName | str, RawMessage | ReplayData], None
    ]:
        if not self.streaming_dataflows:
            raise RuntimeError("No streaming dataflow to iterate over")
        # get the first dataflow, doesn't matter, they share the same faucet
        dataflow = self.streaming_dataflows[0]
        faucet = dataflow.faucet
        queue: asyncio.Queue[
            tuple[WebSocketName | str, RawMessage | ReplayData] | None
        ] = faucet.streaming_queue

        async def _iter():
            async with asyncio.TaskGroup() as task_group:
                producer = task_group.create_task(self.run_async())  # noqa: F841
                while True:
                    # try:
                    msg = await queue.get()
                    # except asyncio.CancelledError:
                    #     _ = producer.cancel()
                    #     break
                    if msg is None:  # sentinel from faucet.close_stream()
                        break
                    yield msg

        return _iter()

    def _create_stream_dataflow(
        self,
        user_callback: Callable[
            [WebSocketName | str, RawMessage | ReplayData],
            Awaitable[None] | None,
        ]
        | None = None,
    ) -> DataFlow:
        request: MarketFeedStreamRequest = cast(
            "MarketFeedStreamRequest", self._get_current_request()
        )
        self.logger.debug(
            f"{request.name}:\n{request}\n", style=TextStyle.BOLD + RichColor.GREEN
        )
        data_model = request.to_data_model()

        # TODO(streaming refactor): one faucet per stream request + one stream hub per feed/stream API
        #   - Faucet (per request): holds the source, the request and _stream_impl.
        #     Subscribes its own channels through the hub (add_channel with request.data_resolution),
        #     registers for its channel keys and unsubscribes on close.
        #     In replay, plays its own request's data (replay's add_channel in _stream_impl misses data_resolution).
        #   - Hub (shared): owns the connection (connect on the first faucet open, disconnect after the last close),
        #     the single stream-API callback, channel_key -> faucet routing, the __aiter__ queue,
        #     the user callback and zmq. Merges multiple replays into one time-ordered stream.
        # Until then, one faucet (created from the first stream request) is shared by all stream requests.
        if self.streaming_dataflows:
            existing_dataflow = cast("DataFlow", self.streaming_dataflows[0])
            faucet: Faucet = existing_dataflow.faucet
        else:
            faucet: Faucet = cast(
                "Faucet",
                self._create_faucet(
                    request=request,
                    extract_func=(
                        lambda data_model, faucet_callback, handler: self._stream_impl(
                            data_model,
                            faucet_callback,
                            handler=handler,
                            replay_pace=request.replay_pace,
                        )
                    ),
                ),
            )
        dataflow = cast(
            "DataFlow",
            self._create_dataflow(
                faucet=faucet,
                data_model=data_model,
                default_transformations=self._get_default_transformations(request),
            ),
        )
        dataflow.set_stream_options(
            flush_interval=request.flush_interval,
            store_incremental_bars=request.store_incremental_bars,
        )

        if user_callback:
            faucet.set_user_callback(user_callback)
        stream_api = self._get_stream_api(request.env)
        channel_key = cast(
            "ChannelKey",
            stream_api.add_channel(data_model, data_resolution=request.data_resolution),
        )
        faucet.bind_channel_key_to_dataflow(channel_key, dataflow)
        self._dataflows[request] = [dataflow]
        return dataflow

    def _optimize_stream_ios(self, dataflows: list[DataFlow]) -> None:
        """Compacts the small files the streamed writes left, once per io.

        Without the engine nothing else compacts them, see DataEngine.
        """
        from pfeed.io.table_io import TableIO

        # TODO: skip when run by the engine, once the engine compacts at each UTC day change
        ios = {
            id(handler.io): handler.io
            for dataflow in dataflows
            if (handler := dataflow.handler) is not None and handler.sink is not None
        }
        for io in ios.values():
            # only table IOs can append, so only they can store streamed data
            if not isinstance(io, TableIO):
                continue
            try:
                io.optimize()
            except Exception:
                self.logger.exception(f"Error optimizing {io!r}:")

    async def _run_stream_dataflows(self) -> list[DataFlow]:
        self._prepare_before_run()

        # Snapshot dataflows up front: streaming runs them all concurrently and we need a
        # stable list to drive both per-task wrapping and end_stream teardown. This also
        # survives `_cleanup_after_run`, so the run can still return them.
        last_run_dataflows: list[DataFlow] = list(self.dataflows)

        async def _run_dataflows():
            try:
                _ = await asyncio.gather(
                    *[dataflow.run_stream(flow_type="native") for dataflow in last_run_dataflows]
                )
            except asyncio.CancelledError:
                self.logger.warning(
                    f"{self.name} dataflows were cancelled, ending streams..."
                )
            finally:
                # end_stream() must run for every dataflow for proper teardown
                # (sentinel into the streaming queue + ZMQ termination);
                # it records its own errors, so one bad teardown doesn't skip the rest.
                _ = await asyncio.gather(*[df.end_stream() for df in last_run_dataflows])
                self._optimize_stream_ios(last_run_dataflows)

        try:
            if self._is_using_ray():
                import ray
                import zmq
                from ray.util.queue import Queue

                from pfeed.streaming.zeromq import (
                    ZeroMQ,
                    ZeroMQDataChannel,
                    ZeroMQSignal,
                )
                from pfeed.utils.ray import (
                    ray_logging_context,
                    setup_logger_in_ray_task,
                    setup_ray,
                )

                setup_ray()

                @ray.remote
                def ray_task(
                    logger_name: str,
                    log_queue: Queue,
                    feed_name: str,
                    worker_name: str,
                    transformations_per_dataflow: dict[
                        DataFlowName, list[Callable[[StreamingData], StreamingData]]
                    ],
                    handlers_per_dataflow: dict[DataFlowName, BaseDataHandler | None],
                    write_stream_kwargs_per_dataflow: dict[DataFlowName, dict[str, Any]],
                    ports_to_connect: dict[Literal["sender", "receiver"], set[int]],
                    ready_queue: Queue,
                ):
                    logger = setup_logger_in_ray_task(logger_name, log_queue)
                    logger.debug(f"Ray {worker_name} started")

                    try:
                        msg_queue = ZeroMQ(
                            name=f"{feed_name}.stream.{worker_name}",
                            logger=logger,
                            sender_type=zmq.PUSH,
                            receiver_type=zmq.DEALER,
                            sender_method="connect",
                        )
                        msg_queue.receiver.setsockopt(
                            zmq.IDENTITY, worker_name.encode()
                        )
                        for sender_or_receiver, ports in ports_to_connect.items():
                            for port in ports:
                                msg_queue.connect(
                                    getattr(msg_queue, sender_or_receiver), port
                                )

                        ready_queue.put(worker_name)
                        is_data_engine_running = "sender" in ports_to_connect
                        while True:
                            msg = msg_queue.recv()
                            if msg is None:
                                continue
                            channel, topic, _data, msg_ts = msg
                            if channel == ZeroMQDataChannel.signal:
                                signal: ZeroMQSignal = _data
                                if signal == ZeroMQSignal.STOP:
                                    # sender_name = topic
                                    # this worker's copies of the handlers hold their own buffered rows
                                    for handler in handlers_per_dataflow.values():
                                        if handler is not None and handler.sink is not None:
                                            handler.flush()
                                    logger.debug(
                                        f"Ray {worker_name} received STOP signal, terminating..."
                                    )
                                    break
                            else:
                                dataflow_name, data = (
                                    _data  # sent from _run_stream_etl() in dataflow.py
                                )
                                transformations = transformations_per_dataflow[
                                    dataflow_name
                                ]
                                for transform in transformations:
                                    data: StreamingData = transform(data)
                                    assert data is not None, (
                                        f"transform function {transform} should return transformed data, but got None"
                                    )

                                if is_data_engine_running:
                                    msg_queue.send(
                                        channel=channel, topic=topic, data=data
                                    )

                                if handler := handlers_per_dataflow[dataflow_name]:
                                    handler.write_stream(
                                        data,
                                        **write_stream_kwargs_per_dataflow[dataflow_name],
                                    )
                        msg_queue.terminate()
                    except Exception:
                        logger.exception(f"Error in streaming Ray {worker_name}:")

                # each stream worker holds a CPU for the whole stream, workers beyond Ray's free CPUs would never start
                num_cpus = max(1, int(ray.available_resources().get("CPU", 1)))
                num_workers = min(self._num_workers, len(last_run_dataflows), num_cpus)
                futures: list[ray.ObjectRef[None]] = []
                try:
                    with ray_logging_context(self.logger) as log_queue:
                        try:
                            # Distribute dataflows' transformations across workers
                            transformations_per_worker: dict[
                                WorkerName,
                                dict[
                                    DataFlowName,
                                    list[Callable[[StreamingData], StreamingData]],
                                ],
                            ] = defaultdict(dict)
                            handlers_per_worker: dict[
                                WorkerName, dict[DataFlowName, BaseDataHandler | None]
                            ] = defaultdict(dict)
                            write_stream_kwargs_per_worker: dict[
                                WorkerName, dict[DataFlowName, dict[str, Any]]
                            ] = defaultdict(dict)
                            ports_to_connect: dict[
                                WorkerName,
                                dict[Literal["sender", "receiver"], set[int]],
                            ] = defaultdict(lambda: defaultdict(set))
                            for i, dataflow in enumerate(last_run_dataflows):
                                worker_num: int = i % num_workers
                                worker_num += (
                                    1  # convert to 1-indexed, i.e. starting from 1
                                )
                                worker_name = _create_worker_name(worker_num)
                                dataflow.set_stream_worker(worker_name)
                                transformations_per_worker[worker_name][
                                    dataflow.name
                                ] = cast(
                                    "list[Callable[[StreamingData], StreamingData]]",
                                    dataflow._transformations,
                                )
                                handlers_per_worker[worker_name][dataflow.name] = (
                                    dataflow.handler
                                )
                                write_stream_kwargs_per_worker[worker_name][
                                    dataflow.name
                                ] = dataflow._write_stream_kwargs
                                # get ports in use for dataflow's ZMQ.ROUTER
                                assert dataflow.faucet.msg_queue is not None, (
                                    f"{dataflow.name} _msg_queue is not set"
                                )
                                dataflow_zmq: ZeroMQ = dataflow.faucet.msg_queue
                                ports_in_use: list[int] = dataflow_zmq.get_ports_in_use(
                                    dataflow_zmq.sender
                                )
                                ports_to_connect[worker_name]["receiver"].update(
                                    ports_in_use
                                )
                                # get ports in use for engine's ZMQ.PULL
                                if self._engine:
                                    engine_zmq: ZeroMQ = cast(
                                        ZeroMQ, self._engine._msg_queue
                                    )
                                    ports_in_use: list[int] = (
                                        engine_zmq.get_ports_in_use(engine_zmq.receiver)
                                    )
                                    ports_to_connect[worker_name]["sender"].update(
                                        ports_in_use
                                    )

                            worker_names = [
                                _create_worker_name(worker_num)
                                for worker_num in range(1, num_workers + 1)
                            ]
                            ready_queue = Queue()  # let ray worker notify the main thread that it's ready to receive messages

                            # start ray workers
                            futures = cast(
                                list[ray.ObjectRef[None]],
                                [
                                    ray_task.remote(
                                        logger_name=self.logger.name,
                                        log_queue=log_queue,
                                        feed_name=self.name.value,
                                        worker_name=worker_name,
                                        transformations_per_dataflow=transformations_per_worker[
                                            worker_name
                                        ],
                                        handlers_per_dataflow=handlers_per_worker[
                                            worker_name
                                        ],
                                        write_stream_kwargs_per_dataflow=write_stream_kwargs_per_worker[
                                            worker_name
                                        ],
                                        ports_to_connect=ports_to_connect[worker_name],
                                        ready_queue=ready_queue,
                                    )
                                    for worker_name in worker_names
                                ],
                            )

                            # wait for ray workers to be ready
                            timeout = 10
                            while timeout:
                                timeout -= 1
                                worker_name = ready_queue.get(timeout=5)
                                worker_names.remove(worker_name)
                                is_workers_ready = not worker_names
                                if is_workers_ready:
                                    break
                            else:
                                raise RuntimeError(
                                    "Timeout: Not all workers reported ready"
                                )

                            # start streaming
                            await _run_dataflows()
                        except KeyboardInterrupt:
                            self.logger.warning(
                                f"KeyboardInterrupt received, stopping {self.name} dataflows..."
                            )
                        except Exception:
                            self.logger.exception(
                                f"Error in running {self.name} dataflows:"
                            )
                finally:
                    # Always wait for Ray workers to drain (they exit on ZMQ STOP from
                    # faucet teardown), even if setup or streaming raised.
                    self.logger.debug("waiting for ray tasks to finish...")
                    if futures:
                        _ = ray.get(futures)
                    self.logger.debug("ray tasks finished")
                    # NOTE: Do NOT shut down Ray here to avoid interfering with Ray's control in higher-level frameworks that might also be using Ray.
                    # shutdown_ray()
            else:
                try:
                    await _run_dataflows()
                except KeyboardInterrupt:
                    self.logger.warning(
                        f"KeyboardInterrupt received, stopping {self.name} dataflows..."
                    )
                except Exception:
                    self.logger.exception(f"Error in running {self.name} dataflows:")

            # Summary log — mirror `_run_batch_dataflows`. Use `result.error is not None`
            # rather than `not result.success`: streaming dataflows never set a positive
            # result on clean shutdown, so checking `success` would false-flag every
            # cleanly-cancelled stream as failed.
            if failed := [
                dataflow
                for dataflow in last_run_dataflows
                if dataflow.result.error is not None
            ]:
                self.logger.warning(
                    f"{self.name} has {len(failed)} failed streaming dataflows, "
                    + f"check {self.logger.name}.log for more details"
                )

            return last_run_dataflows
        finally:
            # Always tear down state, even if the run aborted.
            # Mirrors the batch runner's finally so an escaping exception can't leave
            # `_is_running=True` and lock the feed against future `.run()` calls.
            self._cleanup_after_run()

    def run(self, use_prefect: bool = False, **prefect_kwargs: Any) -> RunResult | list[DataFlow]:
        if self.streaming_dataflows:
            try:
                _ = asyncio.get_running_loop()
            except (
                RuntimeError
            ):  # if no running loop, asyncio.get_running_loop() will raise RuntimeError
                # No running event loop, safe to use asyncio.run()
                pass
            else:
                raise RuntimeError(
                    "Cannot call feed.run() from within a running event loop.\n"
                    + "Did you mean to call feed.run_async() or forget to set 'pipeline_mode=True'?"
                )
            return asyncio.run(self.run_async())
        else:
            return super().run(use_prefect=use_prefect, **prefect_kwargs)

    async def run_async(self) -> list[DataFlow]:
        if not self.streaming_dataflows:
            raise RuntimeError(
                f"{self.name} run_async() is only for streaming dataflows. Use run() instead."
            )
        return await self._run_stream_dataflows()
