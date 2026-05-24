from __future__ import annotations
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pfeed.enums import DataSink


class SinkConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    sink: DataSink | str = DataSink.DELTALAKE
    flush_interval: int = Field(
        default=100,
        description='''
        Interval in seconds for flushing buffered streaming data to storage. Default is 100 seconds.
        If using deltalake:
        Frequent flushes will reduce write performance and generate many small files
        (e.g. part-00001-0a1fd07c-9479-4a72-8a1e-6aa033456ce3-c000.snappy.parquet).
    ''')
    store_incremental_bars: bool = Field(
        default=False,
        description='''
        If True, every bar update (including partial/in-progress bars) is written to storage.
        Useful for research on intra-bar dynamics, but produces many redundant rows that must
        be deduped on read. If False (default), only closed bars are written.
    ''')

    @field_validator('sink', mode='before')
    def validate_sink(cls, value: DataSink | str) -> DataSink:
        if isinstance(value, str):
            return DataSink[value.upper()]
        return value
