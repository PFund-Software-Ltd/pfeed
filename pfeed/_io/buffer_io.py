from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pathlib import Path
    from io import TextIOWrapper
    from pyarrow.ipc import RecordBatchStreamWriter
    import pyarrow.fs as pa_fs

import os

import pyarrow as pa

from pfeed._io.base_io import BaseIO
from pfeed.enums import StreamMode


class BufferIO(BaseIO):
    BUFFER_FILENAME = 'buffer.arrow'
    
    def __init__(
        self,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None=None,
        stream_mode: StreamMode=StreamMode.FAST,
    ):
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
            stream_mode: SAFE or FAST
                FAST: Data accumulates in memory before writing to Arrow buffer
                      (faster writes, slightly higher risk of data loss)
                SAFE: Data writes to Arrow buffer immediately  
                      (slower writes, minimal risk of data loss)
        '''
        super().__init__(filesystem=filesystem, storage_options=storage_options)
        self._stream_mode: StreamMode = StreamMode[stream_mode.upper()]
        self._buffer_files: dict[Path, TextIOWrapper] = {}
        self._ipc_writers: dict[Path, RecordBatchStreamWriter] = {}
        self._buffer: list[dict] = []
    
    def _open_writers(self, file_path: Path, schema: pa.Schema):
        if file_path not in self._buffer_files:
            self._buffer_files[file_path] = open(file_path, 'wb')
        if file_path not in self._ipc_writers:
            self._ipc_writers[file_path] = pa.ipc.new_stream(self._buffer_files[file_path], schema)
            
    def _close_writers(self, file_path: Path):
        if file_path in self._ipc_writers:
            self._ipc_writers[file_path].close()
            del self._ipc_writers[file_path]
        if file_path in self._buffer_files:
            self._buffer_files[file_path].close()
            del self._buffer_files[file_path]
    
    def write(self, file_path: Path, streaming_data: dict, schema: pa.Schema):
        if self._stream_mode == StreamMode.FAST:
            self._buffer.append(streaming_data)
        elif self._stream_mode == StreamMode.SAFE:
            self._spill_to_disk(file_path, streaming_data, schema)
        else:
            raise ValueError(f'Invalid stream mode: {self._stream_mode}')
            
    def _spill_to_disk(self, file_path: Path, streaming_data: dict, schema: pa.Schema):
        '''Spill in-memory buffer to buffer.arrow'''
        self._open_writers(file_path, schema)
        row = { name: [ streaming_data[name] ] for name in schema.names }  # make each column a list
        batch = pa.record_batch(row, schema=schema)
        self._ipc_writers[file_path].write_batch(batch)
        buffer_file = self._buffer_files[file_path]
        buffer_file.flush()  # flush python buffer to OS buffer
        os.fsync(buffer_file.fileno())  # flush OS buffer to disk
        # write_size = os.fstat(buffer_file.fileno()).st_size
            
    def read(self, file_path: Path, schema: pa.Schema | None=None) -> pa.Table:
        if self._stream_mode == StreamMode.FAST:
            assert schema is not None, 'schema is required for reading in fast mode'
            return pa.Table.from_pylist(self._buffer, schema=schema)
        elif self._stream_mode == StreamMode.SAFE:
            self._close_writers(file_path)
            with open(file_path, "rb") as f:
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

    def clear(self, file_path: Path):
        '''Clear buffer or the buffer.arrow file'''
        if self._stream_mode == StreamMode.FAST:
            self._buffer.clear()
        elif self._stream_mode == StreamMode.SAFE:
            if file_path.exists():
                file_path.unlink()