from pydantic import BaseModel, Field

from pfeed.enums import StreamMode


class StreamingSettings(BaseModel):
    mode: StreamMode = Field(default=StreamMode.FAST)
    flush_interval: int = Field(default=100, description='''
        Interval in seconds for flushing buffered streaming data to storage. Default is 100 seconds.
        If using deltalake:
        Frequent flushes will reduce write performance and generate many small files 
        (e.g. part-00001-0a1fd07c-9479-4a72-8a1e-6aa033456ce3-c000.snappy.parquet).
        Infrequent flushes create larger files but increase data loss risk during crashes when using FAST stream_mode.
        This is expected to be fine-tuned based on the actual use case.
    ''')