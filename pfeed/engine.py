from __future__ import annotations
from typing import TYPE_CHECKING, Coroutine, AsyncGenerator
if TYPE_CHECKING:
    from pfeed.typing import tDataTool, tDataSource, tDataCategory, GenericData
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.messaging.zeromq import ZeroMQ

import asyncio
import logging
from threading import Thread

from pfund import print_warning
import pfeed as pe
from pfeed.enums import DataSource, DataCategory


logger = logging.getLogger('pfeed')


# NOTE: only data engine has the ability to run background tasks in pfeed
class DataEngine:
    STREAMING_QUEUE_MAXSIZE = 1000

    def __init__(
        self,
        data_tool: tDataTool='polars',
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
        # TODO
        # backfill: bool=True,
    ):
        '''
        Args:
            backfill: Whether to backfill the data during streaming.
                only matters to streaming dataflows (i.e. feed.stream())
        '''
        self._params = {k: v for k, v in locals().items() if k not in ['self', 'backfill']}
        # NOTE: pipeline_mode must be turned on for the engine to work so that users can add tasks to the feed
        self._params['pipeline_mode'] = True
        self._feeds: list[BaseFeed] = []
        self._is_running: bool = False
        self._msg_queue: ZeroMQ | None = None
        self._zmq_thread: Thread | None = None
        # TODO
        # self._backfill: bool = backfill
        if use_ray:
            self._setup_messaging()
    
    def is_running(self) -> bool:
        return self._is_running
    
    @property
    def feeds(self) -> list[BaseFeed]:
        return self._feeds
    
    def _setup_messaging(self, zmq_url: str | None=None, zmq_sender_port: int | None=None, zmq_receiver_port: int | None=None):
        import zmq
        from pfeed.messaging.zeromq import ZeroMQ
        self._msg_queue = ZeroMQ(
            name='data_engine',
            logger=logger,
            io_threads=2,
            sender_type=zmq.XPUB,
            receiver_type=zmq.PULL,
            receiver_method='bind',
        )
        self._msg_queue.bind(self._msg_queue.sender, port=zmq_sender_port, url=zmq_url or ZeroMQ.DEFAULT_URL)
        self._msg_queue.bind(self._msg_queue.receiver, port=zmq_receiver_port, url=zmq_url or ZeroMQ.DEFAULT_URL)
    
    def _run_zmq_loop(self):
        '''receive messages from Ray workers'''
        while self.is_running():
            msg = self._msg_queue.recv()
            if msg is None:
                continue
            # NOTE: received transformed/standardized data from Ray workers
            channel, topic, data, msg_ts = msg
            # send to subscribers, e.g. strategies, models in pfund
            self._msg_queue.send(channel=channel, topic=topic, data=data)
        self._msg_queue.terminate()
    
    # TODO: async background task
    def backfill(self):
        raise NotImplementedError('Backfill is not implemented yet')
    
    def add_feed(self, data_source: tDataSource, data_category: tDataCategory, **kwargs) -> BaseFeed:
        '''
        Args:
            kwargs: kwargs for the data client to override the default params in the engine
        '''
        assert 'pipeline_mode' not in kwargs, 'pipeline_mode is True by default and cannot be overridden'
        params = self._params.copy()
        params.update(kwargs)
        feed: BaseFeed = pe.create_feed(data_source=data_source, data_category=data_category, **params)
        feed._set_engine(self)
        self._feeds.append(feed)
        return feed
    
    def get_feeds(self, data_source: tDataSource | None=None, data_category: tDataCategory | None=None) -> list[BaseFeed]:
        """Filter feeds by source and/or category"""
        filtered_feeds = self._feeds
        if data_source:
            filtered_feeds = [f for f in filtered_feeds if f.data_source.name == DataSource[data_source.upper()]]
        if data_category:
            filtered_feeds = [f for f in filtered_feeds if f.data_model_class.data_category == DataCategory[data_category.upper()]]
        return filtered_feeds
    
    def _is_streaming_feeds(self) -> bool:
        # either all streaming dataflows or all batch dataflows, cannot mix them
        if is_streaming_feeds := any(feed.streaming_dataflows for feed in self._feeds):
            assert all(feed.streaming_dataflows for feed in self._feeds), 'All feeds must be streaming feeds if any feed is streaming'
        return is_streaming_feeds
    
    def _eager_run(
        self, 
        ray_kwargs: dict | None=None, 
        prefect_kwargs: dict | None=None, 
        include_metadata: bool=False
    ) -> dict[BaseFeed, GenericData] | list[Coroutine]:
        if self.is_running():
            raise RuntimeError('Data Engine is already running, cannot run again')
        self._is_running = True
        if not self._is_streaming_feeds():
            result: dict[BaseFeed, GenericData] = {}
            for feed in self._feeds:
                data = feed.run(prefect_kwargs=prefect_kwargs, include_metadata=include_metadata, **ray_kwargs)
                result[feed] = data
        else:
            result: list[Coroutine] = []
            for feed in self._feeds:
                coro: Coroutine = feed.run_async(prefect_kwargs=prefect_kwargs, include_metadata=include_metadata, **ray_kwargs)
                result.append(coro)
        return result
    
    def run(self, prefect_kwargs: dict | None=None, include_metadata: bool=False, **ray_kwargs) -> dict[BaseFeed, GenericData] | None:
        if not self._is_streaming_feeds():
            result: dict[BaseFeed, GenericData] = self._eager_run(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs, include_metadata=include_metadata)
            return result
        else:
            try:
                asyncio.get_running_loop()
            except RuntimeError:  # if no running loop, asyncio.get_running_loop() will raise RuntimeError
                # No running event loop, safe to use asyncio.run()
                pass
            else:
                print_warning("Cannot call engine.run() from within a running event loop. Did you mean to call engine.run_async()?")
                return
            return asyncio.run(self.run_async())
    
    async def run_async(self, prefect_kwargs: dict | None=None, include_metadata: bool=False, **ray_kwargs) -> None:
        assert self._is_streaming_feeds(), 'Only streaming feeds can be run asynchronously'
        result: list[Coroutine] = self._eager_run(ray_kwargs=ray_kwargs, prefect_kwargs=prefect_kwargs, include_metadata=include_metadata)
        if self._params['use_ray']:
            self._zmq_thread = Thread(target=self._run_zmq_loop, daemon=True)
            self._zmq_thread.start()
        try:
            return await asyncio.gather(*result)
        except asyncio.CancelledError:
            logger.warning('Data Engine feeds were cancelled')
            if self.is_running():
                self.end()
    
    def end(self):
        if not self.is_running():
            logger.debug('Data Engine is not running, skipping end()')
            return
        logger.debug('Data Engine is ending')
        self._is_running = False
        if self._zmq_thread:
            self._zmq_thread.join(timeout=10)
            if self._zmq_thread.is_alive():
                logger.debug("ZMQ thread is still running after timeout")
            else:
                logger.debug("ZMQ thread finished")
    
    def __aiter__(self) -> AsyncGenerator[Coroutine, None]:
        assert self._is_streaming_feeds(), 'Only streaming feeds support async iteration'
        
        async def _iter():
            queue = asyncio.Queue(maxsize=self.STREAMING_QUEUE_MAXSIZE)
            
            async def _producer(feed):
                """Put messages from a feed into the shared queue"""
                try:
                    async for msg in feed:
                        if queue.full():
                            logger.warning(f"Streaming queue full, dropping oldest message - consider increasing maxsize (current: {self.STREAMING_QUEUE_MAXSIZE}) or improving consumer speed")
                            queue.get_nowait()  # Remove oldest
                        await queue.put((feed, msg))
                except Exception:
                    logger.exception(f"Exception in feed {feed.name}:")
                finally:
                    await queue.put((feed, None))  # Sentinel for this feed
                    
            producers = [asyncio.create_task(_producer(feed)) for feed in self._feeds]
            completed_feeds = 0
            
            try:
                while completed_feeds < len(self._feeds):
                    feed, msg = await queue.get()
                    
                    if msg is None:  # Feed completed
                        completed_feeds += 1
                    else:
                        yield feed, msg
            finally:
                # Clean up
                for producer in producers:
                    if not producer.done():
                        producer.cancel()
            
        return _iter()
        