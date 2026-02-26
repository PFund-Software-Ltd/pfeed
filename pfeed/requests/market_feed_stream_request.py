from typing import Literal

from pydantic import Field, field_validator

from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, StreamMode


class MarketFeedStreamRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.stream] = ExtractType.stream
    stream_mode: StreamMode = Field(
        default=StreamMode.FAST,
        description='''
        Stream mode for streaming data.
        If "FAST" is chosen, streaming data will be cached to memory to a certain amount before writing to disk,
        faster write speed, but data loss risk will increase.
        If "SAFE" is chosen, streaming data will be written to disk immediately,
        slower write speed, but data loss risk will be minimized.
    ''')
    flush_interval: int = Field(
        default=100, 
        description='''
        Interval in seconds for flushing buffered streaming data to storage. Default is 100 seconds.
        If using deltalake:
        Frequent flushes will reduce write performance and generate many small files 
        (e.g. part-00001-0a1fd07c-9479-4a72-8a1e-6aa033456ce3-c000.snappy.parquet).
        Infrequent flushes create larger files but increase data loss risk during crashes when using FAST stream_mode.
        This is expected to be fine-tuned based on the actual use case.
    ''')

    @field_validator("stream_mode", mode="before")
    @classmethod
    def create_stream_mode(cls, v: StreamMode | str) -> StreamMode:
        if isinstance(v, str):
            return StreamMode[v.upper()]
        return v

    def is_streaming(self) -> bool:
        return True
