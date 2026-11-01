from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from pfund_blocks.typing import Message, ProductName
    from ray.util.queue import Queue

    from pfeed.base.feed import BaseFeed
    from pfeed.dataflow.dataflow import DataFlow
    from pfeed.dataflow.result import RunResult
    from pfeed.market.data_model import MarketDataModel
    from pfeed.market.requests.stream_request import MarketFeedStreamRequest

    # NOTE: only for the type checker, so that it knows the mixin's self is a feed; at runtime the mixin inherits an empty class.
    # the type arguments are Any because a specific request type conflicts with the one the feed passes to MarketFeed,
    # e.g. BybitMarketFeed uses MarketFeedBaseRequest
    _FeedBase = BaseFeed[Any, Any, Any]
else:
    # NOTE: not object, a generic class can't list object as a base, its MRO with Generic is inconsistent
    class _FeedBase: ...

import asyncio
import inspect

from pfund_blocks.enums.env import Environment


class StreamingFeedMixin[RequestT: MarketFeedStreamRequest, DataModelT: MarketDataModel](_FeedBase):
    QUEUE_MAXSIZE: ClassVar[int] = 1000

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        # NOTE: one queue per dataflow
        self._queues: dict[tuple[ProductName, str], asyncio.Queue[Any]] = {}
        self._user_callback: Callable[[Message], Awaitable[None] | None] | None = None

    def __aiter__(
        self,
    ) -> AsyncGenerator[tuple[Message], None]:
        if not self.is_streaming():
            raise RuntimeError("No streaming dataflow to iterate over")
        # get the first dataflow, doesn't matter, they share the same faucet
        dataflow = self.dataflows[0]
        faucet = dataflow.faucet
        queue: asyncio.Queue[
            tuple[WebSocketName | str, RawMessage | ReplayData] | None
        ] = faucet.streaming_queue
        # NOTE: this snippet is copied from faucet as a reminder that we will need to handle the case when the queue is full
        # it is NOT placed in order in this __aiter__ function
        # put msg to streaming queue, which is mainly used in feed's __aiter__()
        # if self._streaming_queue:
        #     if self._streaming_queue.full():
        #         self._logger.warning(
        #             "Streaming queue full, dropping oldest message - consider increasing maxsize "
        #             + f"(current: {self.STREAMING_QUEUE_MAXSIZE}) or improving consumer speed"
        #         )
        #         _ = self._streaming_queue.get_nowait()  # Remove oldest
        #     await self._streaming_queue.put((source, data))

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

    @staticmethod
    def _create_queue_key(request: RequestT) -> tuple[ProductName, str]:
        return (request.product.name, repr(request.data_resolution))

    def _create_queue(self, request: RequestT) -> None:
        key = self._create_queue_key(request)
        if key in self._queues:
            raise ValueError(f"{key} is already being streamed")
        self._queues[key] = asyncio.Queue(maxsize=self.QUEUE_MAXSIZE)

    async def _callback(self, msg: Message):
        if self._user_callback:
            result = self._user_callback(msg)
            if inspect.isawaitable(result):
                await result
        # NOTE: error messages can have no product/resolution, so they have no queue
        if (queue := self._queues.get((msg["product"], msg["resolution"]))) is not None:
            await queue.put(msg)

    def _set_user_callback(self, callback: Callable[[Message], Awaitable[None] | None]):
        # NOTE: a connection has only one callback, a different one would silently replace the existing one
        if self._user_callback and self._user_callback != callback:
            from pfeed.utils import is_lambda

            msg = (
                f"streaming callback is already set, existing callback function: {self._user_callback}\n"
                + f"new callback function: {callback}"
            )
            if is_lambda(callback) or is_lambda(self._user_callback):
                msg += (
                    "\nNote: lambda functions are always distinct objects even with identical code, "
                    + "consider using a named function (def) instead of a lambda to avoid this"
                )
            raise ValueError(msg)
        self._user_callback = callback

    def _create_stream_dataflow(
        self,
        *,
        extract_func: Callable[..., Any],
        data_model: DataModelT,
        callback: Callable[[Message], Awaitable[None] | None] | None = None,
    ) -> DataFlow:
        from pfund_kit.style import RichColor, TextStyle

        request: RequestT = self._get_current_request()
        existing_dataflow = self.dataflows[0] if self.dataflows else None
        existing_faucet = existing_dataflow.faucet if existing_dataflow else None
        existing_request = existing_faucet.request if existing_faucet else None
        if existing_request and request.env != existing_request.env:
            raise ValueError(f"cannot stream {request.env} and {existing_request.env} in the same feed")
        self.logger.debug(f"{request.name}:\n{request}\n", style=TextStyle.BOLD + RichColor.GREEN)

        if callback:
            self._set_user_callback(callback)
            
        if request.env in (Environment.PAPER, Environment.LIVE):
            stream_api = self.data_source.get_stream_api(request.env)
            stream_api.add_channel(request.product, request.data_resolution)
            stream_api.set_callback(self._callback)
            self._create_queue(request)
            
        faucet = existing_faucet or self._create_faucet(request=request, extract_func=extract_func)
        default_transformations = self._get_default_transformations(request)
        dataflow = self._create_dataflow(faucet=faucet, data_model=data_model, default_transformations=default_transformations)
        self._dataflows[request] = [dataflow]
        return dataflow

    async def _run_stream_dataflows(self) -> list[DataFlow]:
        self._prepare_before_run()
        last_run_dataflows: list[DataFlow] = []

        async def _run_dataflows():
            runs = [
                dataflow.run_stream(queue=self._queues[self._create_queue_key(request)])
                for request, dataflows in self._dataflows.items()
                for dataflow in dataflows
            ]
            try:
                # NOTE: watches must come after runs, wait_until_disconnected() returns right away if connect() hasn't been called,
                # gather() starts them in order, and a dataflow's run_stream() reaches connect() before its first wait
                await asyncio.gather(*runs, return_exceptions=True)
            except asyncio.CancelledError:
                self.logger.warning(f"{self.name} dataflows were cancelled, ending streams...")
            finally:
                # end_stream() must run for every dataflow for proper teardown;
                # it records its own errors, so one bad teardown doesn't skip the rest.
                await asyncio.gather(*[df.end_stream() for df in self.dataflows])

        try:
            # TODO
            if self._is_using_ray():
                import ray
                from pfund_kit.utils.ray import (
                    ray_logging_context,
                    setup_logger_in_ray_task,
                    setup_ray,
                )

                setup_ray()

                @ray.remote
                def ray_task(
                    logger_name: str,
                    log_queue: Queue,
                ) -> None:
                    setup_logger_in_ray_task(logger_name, log_queue)

                with ray_logging_context(self.logger) as log_queue:
                    try:
                        pass
                    except BaseException:
                        raise
            else:
                last_run_dataflows.extend(self.dataflows)
                await _run_dataflows()

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
