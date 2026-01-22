from __future__ import annotations
from typing import TYPE_CHECKING, AsyncGenerator, TypeAlias, Callable, Literal, Awaitable
if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.dataflow.faucet import Faucet
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.typing import StreamingData, GenericData
    from pfeed.dataflow.sink import Sink
    from pfeed.messaging.zeromq import ZeroMQ, ZeroMQSignal
    from pfeed.enums import StreamMode, ExtractType

import asyncio
from abc import abstractmethod
from collections import defaultdict

from pfund_kit.style import cprint, TextStyle, RichColor
from pfeed.streaming_settings import StreamingSettings


class StreamingFeedMixin:
    def __init__(self):
        self._streaming_settings: StreamingSettings | None = None
        
    @abstractmethod
    def stream(self, *args, **kwargs) -> BaseFeed:
        pass
    
    @abstractmethod
    def _stream_impl(self, data_model: BaseDataModel) -> GenericData | None:
        pass

    @abstractmethod
    def _close_stream(self):
        pass
    
    @abstractmethod
    def _add_default_transformations_to_stream(self, *args, **kwargs):
        pass

    @property
    def streaming_dataflows(self) -> list[DataFlow]:
        return [dataflow for dataflow in self._dataflows if dataflow.is_streaming()]
    
    def _create_streaming_settings(self, mode: StreamMode, flush_interval: int):
        self._streaming_settings = StreamingSettings(mode=mode, flush_interval=flush_interval)
    
    def __aiter__(self) -> AsyncGenerator:
        if not self.streaming_dataflows:
            raise RuntimeError("No streaming dataflow to iterate over")
        # get the first dataflow, doesn't matter, they share the same faucet
        streaming_dataflow = self.streaming_dataflows[0]
        queue: asyncio.Queue = streaming_dataflow.faucet.get_streaming_queue()
        async def _iter():
            async with asyncio.TaskGroup() as task_group:
                producer = task_group.create_task(self.run_async())
                while True:
                    try:
                        msg = await queue.get()
                    except asyncio.CancelledError:
                        producer.cancel()
                        break
                    if msg is None:          # sentinel from faucet.close_stream()
                        break
                    yield msg
                await producer
        return _iter()
    
    def _run_stream(
        self,
        data_model: BaseDataModel,
        add_default_transformations: Callable,
        load_to_storage: Callable | None,
        callback: Callable[[dict], Awaitable[None] | None] | None,
    ) -> None | BaseFeed:
        
        
        # FIXME: move this to _create_stream_dataflow(), refer to _create_batch_dataflows()
        # reuse existing faucet for streaming dataflows since they share the same extract_func
        if self.streaming_dataflows:
            existing_dataflow = self.streaming_dataflows[0]
            faucet: Faucet = existing_dataflow.faucet
        else:
            faucet: Faucet = self._create_faucet(
                data_model=data_model,
                extract_func=self._stream_impl,
                extract_type=ExtractType.stream,
                close_stream=self._close_stream,
            )
        dataflow: DataFlow = self._create_dataflow(data_model=data_model, faucet=faucet)
        self._dataflows.append(dataflow)
        
        
        if callback:
            faucet.set_streaming_callback(callback)
        faucet.bind_data_model_to_dataflow(data_model, dataflow)
        add_default_transformations()
        if load_to_storage:
            load_to_storage()
        if not self._pipeline_mode:
            return self.run()
        else:
            return self

    async def _run_stream_dataflows(self):
        async def _run_dataflows():
            try:
                await asyncio.gather(*[dataflow.run_stream(flow_type='native') for dataflow in self._dataflows])
            except asyncio.CancelledError:
                self.logger.warning(f'{self.name} dataflows were cancelled, ending streams...')
                await asyncio.gather(*[dataflow.end_stream() for dataflow in self._dataflows])
                    
        WorkerName: TypeAlias = str
        DataFlowName: TypeAlias = str

        self._auto_load()

        if self._ray_kwargs:
            import ray
            from ray.util.queue import Queue
            from pfeed.utils.logging import setup_logger_in_ray_task, ray_logging_context
            
            import zmq
            from pfeed.messaging.zeromq import ZeroMQ, ZeroMQDataChannel, ZeroMQSignal

            @ray.remote
            def ray_task(
                logger_name: str,
                log_queue: Queue,
                feed_name: str,
                worker_name: str,
                transformations_per_dataflow: dict[DataFlowName, list[Callable]],
                sinks_per_dataflow: dict[DataFlowName, Sink],
                ports_to_connect: dict[Literal['sender', 'receiver'], list[int]],
                ready_queue: Queue,
            ):
                logger = setup_logger_in_ray_task(logger_name, log_queue)
                logger.debug(f"Ray {worker_name} started")

                try:
                    msg_queue = ZeroMQ(
                        name=f'{feed_name}.stream.{worker_name}',
                        logger=logger,
                        sender_type=zmq.PUSH,
                        receiver_type=zmq.DEALER,
                        sender_method='connect',
                    )
                    msg_queue.receiver.setsockopt(zmq.IDENTITY, worker_name.encode())
                    for sender_or_receiver, ports in ports_to_connect.items():
                        for port in ports:
                            msg_queue.connect(getattr(msg_queue, sender_or_receiver), port)

                    ready_queue.put(worker_name)
                    is_data_engine_running = 'sender' in ports_to_connect
                    num_senders = len(msg_queue.get_ports_in_use(msg_queue.receiver))
                    while True:
                        msg = msg_queue.recv()
                        if msg is None:
                            continue
                        channel, topic, data, msg_ts = msg
                        if channel == ZeroMQDataChannel.signal:
                            signal: ZeroMQSignal = data
                            if signal == ZeroMQSignal.STOP:
                                sender_name = topic
                                num_senders -= 1
                                logger.debug(f'Ray {worker_name} received STOP signal from {sender_name}, {num_senders} senders left')
                                if num_senders == 0:
                                    logger.debug(f'Ray {worker_name} received STOP signals from all senders, terminating')
                                    break
                        else:
                            dataflow_name, data = data
                            transformations = transformations_per_dataflow[dataflow_name]
                            for transform in transformations:
                                data: StreamingData = transform(data)
                                assert data is not None, f'transform function {transform} should return transformed data, but got None'
                            
                            if is_data_engine_running:
                                msg_queue.send(channel=channel, topic=topic, data=data)
                            
                            if sink := sinks_per_dataflow[dataflow_name]:
                                sink.flush(data, streaming=True)
                    msg_queue.terminate()
                except Exception:
                    logger.exception(f'Error in streaming Ray {worker_name}:')
            
            with ray_logging_context(self.logger) as log_queue:
                try:
                    num_workers = min(self._ray_kwargs['num_cpus'], len(self._dataflows))
                    
                    # Distribute dataflows' transformations across workers
                    def _create_worker_name(worker_num: int) -> str:
                        return f'worker-{worker_num}'
                    transformations_per_worker: dict[WorkerName, dict[DataFlowName, list[Callable]]] = defaultdict(dict)
                    sinks_per_worker: dict[WorkerName, dict[DataFlowName, Sink]] = defaultdict(dict)
                    ports_to_connect: dict[WorkerName, dict[Literal['sender', 'receiver'], list[int]]] = defaultdict(lambda: defaultdict(list))
                    for i, dataflow in enumerate(self._dataflows):
                        worker_num: int = i % num_workers
                        worker_num += 1  # convert to 1-indexed, i.e. starting from 1
                        worker_name = _create_worker_name(worker_num)
                        dataflow._setup_messaging(worker_name)
                        transformations_per_worker[worker_name][dataflow.name] = dataflow._transformations
                        sinks_per_worker[worker_name][dataflow.name] = dataflow._sink
                        # get ports in use for dataflow's ZMQ.ROUTER
                        dataflow_zmq: ZeroMQ = dataflow._msg_queue
                        ports_in_use: list[int] = dataflow_zmq.get_ports_in_use(dataflow_zmq.sender)
                        ports_to_connect[worker_name]['receiver'].extend(ports_in_use)
                        # get ports in use for engine's ZMQ.PULL
                        if self._engine:
                            engine_zmq: ZeroMQ = self._engine._msg_queue
                            ports_in_use: list[int] = engine_zmq.get_ports_in_use(engine_zmq.receiver)
                            ports_to_connect[worker_name]['sender'].extend(ports_in_use)

                    worker_names = [_create_worker_name(worker_num) for worker_num in range(1, num_workers+1)]
                    ready_queue = Queue()  # let ray worker notify the main thread that it's ready to receive messages

                    # start ray workers
                    futures = [
                        ray_task.remote(
                            logger_name=self.logger.name,
                            log_queue=log_queue,
                            feed_name=self.name.value,
                            worker_name=worker_name,
                            transformations_per_dataflow=transformations_per_worker[worker_name],
                            sinks_per_dataflow=sinks_per_worker[worker_name],
                            ports_to_connect=ports_to_connect[worker_name],
                            ready_queue=ready_queue,
                        ) for worker_name in worker_names
                    ]

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
                        raise RuntimeError("Timeout: Not all workers reported ready")
                    
                    # start streaming
                    await _run_dataflows()
                except KeyboardInterrupt:
                    print(f"KeyboardInterrupt received, stopping {self.name} dataflows...")
                except Exception:
                    self.logger.exception(f'Error in running {self.name} dataflows:')
            self.logger.debug('waiting for ray tasks to finish...')
            ray.get(futures)
            self.logger.debug('shutting down ray...')
            self._shutdown_ray()
        else:
            await _run_dataflows()
    
    def run(self, prefect_kwargs: dict | None=None) -> GenericData | None:
        if self.streaming_dataflows:
            try:
                asyncio.get_running_loop()
            except RuntimeError:  # if no running loop, asyncio.get_running_loop() will raise RuntimeError
                # No running event loop, safe to use asyncio.run()
                pass
            else:
                cprint(
                    "Cannot call feed.run() from within a running event loop.\n"
                    "Did you mean to call feed.run_async() or forget to set 'pipeline_mode=True'?",
                    style=str(TextStyle.BOLD + RichColor.RED),
                )
                return
            return asyncio.run(self._run_stream_dataflows())
        else:
            return super().run(prefect_kwargs=prefect_kwargs)

    async def run_async(self, prefect_kwargs: dict | None=None) -> GenericData | None:
        if self.streaming_dataflows:
            return await self._run_stream_dataflows()
        else:
            return super().run(prefect_kwargs=prefect_kwargs)