from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from io import TextIOWrapper
    from pyarrow.ipc import RecordBatchStreamWriter
    from pfeed.utils.file_path import FilePath
    from pfeed._io.base_io import BaseIO

import os
import time
import atexit

import pyarrow as pa

from pfeed.enums import StreamMode


class StreamBuffer:
    FILENAME = 'buffer.arrow'
    
    def __init__(self, io: BaseIO, buffer_path: FilePath, stream_mode: StreamMode=StreamMode.FAST, flush_interval: int=100):
        '''
        Manages a 3-layer buffering system for high-speed streaming data:
        
        Layer 1: In-memory buffer (Python list)
            - Fastest writes possible
            - Data stored in RAM as simple Python dictionaries
            - Risk: Data lost if program crashes
            
        Layer 2: Arrow buffer file (buffer.arrow) 
            - Fast staging area on disk
            - Data moves here when in-memory buffer fills up
            - 6x faster than Delta Lake writes
            - Benefit: Data survives crashes, still very fast
            
        Layer 3: Delta Lake (final storage)
            - Data moves here periodically (every ~100 seconds)
            - Optimized for analytics, queries, and long-term storage
            - Slowest writes but provides features like time travel and ACID transactions
        
        This design keeps streaming writes fast while ensuring data durability and 
        analytics capabilities.
        
        Args:
            io: IO object
            buffer_path: Path to the buffer file
            stream_mode: SAFE or FAST
                if "FAST" is chosen, streaming data will be cached to memory to a certain amount before writing to disk,
                faster write speed, but data loss risk will increase.
                if "SAFE" is chosen, streaming data will be written to disk immediately,
                slower write speed, but data loss risk will be minimized.
            flush_interval: Interval in seconds for flushing buffered streaming data to storage. 
                Default is 100 seconds.
                If using deltalake:
                Frequent flushes will reduce write performance and generate many small files 
                (e.g. part-00001-0a1fd07c-9479-4a72-8a1e-6aa033456ce3-c000.snappy.parquet).
                Infrequent flushes create larger files but increase data loss risk during crashes when using FAST stream_mode.
                This is expected to be fine-tuned based on the actual use case.
        
        '''
        from pfeed.utils.adapter import Adapter
        
        self._io: BaseIO = io
        self._buffer_path: FilePath = buffer_path
        self._file_path: FilePath = buffer_path / self.FILENAME
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        self._stream_mode: StreamMode = stream_mode
        self._flush_interval: int = flush_interval
        self._file: TextIOWrapper | None = None
        self._ipc_writer: RecordBatchStreamWriter | None = None
        self._buffer: list[dict] = []
        self._adapter = Adapter()
        self._message_schema: pa.Schema | None = None
        self._last_flush_ts = time.time()
        self._recover_from_crash()
        atexit.register(self._recover_from_crash)
    
    def _recover_from_crash(self):
        '''
        Recover from crash by reading buffer.arrow and writing it to deltalake
        '''
        if self._stream_mode == StreamMode.SAFE and self._file_path and self._file_path.exists() and self._file_path.stat().st_size != 0:
            self._flush()

    def _open_writer(self):
        if self._file is None:
            self._file = open(self._file_path, 'wb')
        if self._ipc_writer is None:
            self._ipc_writer = pa.ipc.new_stream(self._file, self._message_schema)
            
    def _close_writer(self):
        if self._ipc_writer is not None:
            self._ipc_writer.close()
            self._ipc_writer = None
        if self._file is not None:
            self._file.close()
            self._file = None
    
    def write(self, data: dict, metadata: dict | None=None, partition_by: list[str] | None=None):
        if self._message_schema is None:
            self._create_message_schema(data)
        if self._stream_mode == StreamMode.FAST:
            self._buffer.append(data)
        elif self._stream_mode == StreamMode.SAFE:
            self._spill_to_disk(data)
        else:
            raise ValueError(f'Invalid stream mode: {self._stream_mode}')
        self._flush(metadata=metadata, partition_by=partition_by)
            
    # OPTIMIZE
    def _flush(self, metadata: dict | None=None, partition_by: list[str] | None=None):
        now = time.time()
        if now - self._last_flush_ts <= self._flush_interval:
            return
        buffer_table: pa.Table = self.read()
        self._io.write(
            file_path=self._buffer_path,
            data=buffer_table,
            metadata=metadata,
            partition_by=partition_by,
        )
        self.clear()
        self._last_flush_ts = now
            
    def _spill_to_disk(self, streaming_data: dict):
        '''Spill in-memory buffer to buffer.arrow'''
        self._open_writer()
        row = { name: [ streaming_data[name] ] for name in self._message_schema.names }  # make each column a list
        batch = pa.record_batch(row, schema=self._message_schema)
        self._ipc_writer.write_batch(batch)
        self._file.flush()  # flush python buffer to OS buffer
        os.fsync(self._file.fileno())  # flush OS buffer to disk
        # write_size = os.fstat(self._file.fileno()).st_size
    
    def _create_message_schema(self, data: dict):
        self._message_schema = self._adapter.dict_to_schema(data)
            
    def read(self) -> pa.Table:
        if self._stream_mode == StreamMode.FAST:
            assert self._message_schema is not None, 'schema is required for reading in fast mode'
            return pa.Table.from_pylist(self._buffer, schema=self._message_schema)
        elif self._stream_mode == StreamMode.SAFE:
            self._close_writer()
            with open(self._file_path, "rb") as f:
                with pa.ipc.open_stream(f) as reader:
                    table = reader.read_all()
                    # REVIEW: combine_chunks() could create misaligned memory for Delta Lake FFI
                    # currently if only writing one data per batch, this issue doesn't occur for some reason
                    table = table.combine_chunks()
                    '''More details on the problem:
                    Because the combined table's memory layout doesn't meet the 8-byte alignment requirements for Delta Lake's Rust FFI interface
                    The problem occurs when:
                    1. Multiple small record batches get combined
                    2. combine_chunks() creates a memory layout where buffer addresses aren't 8-byte aligned
                    3. Delta Lake's Rust FFI expects all Arrow buffer pointers to be aligned to 8-byte boundaries
                    Instead, reconstruct the table to ensure proper memory alignment
                    # NOTE: fallback solution when this issue occurs again
                    if table.num_rows > 0:
                        df = table.to_pandas()
                        table = pa.Table.from_pandas(df, preserve_index=False, schema=table.schema)
                    '''
                    return table

    def clear(self):
        '''Clear buffer or the buffer.arrow file'''
        if self._stream_mode == StreamMode.FAST:
            self._buffer.clear()
        elif self._stream_mode == StreamMode.SAFE:
            if self._file_path.exists():
                self._file_path.unlink()