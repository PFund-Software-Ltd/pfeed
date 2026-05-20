from __future__ import annotations
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pfeed.enums import DataSink, StreamMode


class SinkConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    sink: DataSink | str = DataSink.DELTALAKE
    stream_mode: StreamMode | str = Field(
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

    @field_validator('sink', mode='before')
    def validate_sink(cls, value: DataSink | str) -> DataSink:
        if isinstance(value, str):
            return DataSink[value.upper()]
        return value

    @field_validator('stream_mode', mode='before')
    def validate_stream_mode(cls, value: StreamMode | str) -> StreamMode:
        if isinstance(value, str):
            return StreamMode[value.upper()]
        return value
