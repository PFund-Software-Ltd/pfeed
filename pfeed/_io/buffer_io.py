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
    # REVIEW: make it a param?
    MAX_BUFFER_SIZE = 100  # number of data
    
    def __init__(
        self,
        filesystem: pa_fs.FileSystem,
        storage_options: dict | None=None,
        stream_mode: StreamMode=StreamMode.FAST,
    ):
        '''
        Writes streaming data to buffer.arrow, which will be flushed to deltalake when the target file size is reached.
        This is a better alternative to writing to deltalake directly, because it can reduce the number of writes to deltalake,
        which is a time-consuming operation.
        Args:
            stream_mode: SAFE or FAST
                if "FAST" is chosen, streaming data will be cached to memory to a certain amount before writing to disk,
                faster write speed, but data loss risk will increase.
                if "SAFE" is chosen, streaming data will be written to disk immediately,
                slower write speed, but data loss risk will be minimized.
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
        self._buffer.append(streaming_data)
        if self._stream_mode == StreamMode.FAST:
            if len(self._buffer) >= self.MAX_BUFFER_SIZE:
                self.spill_to_disk(file_path, schema)
        elif self._stream_mode == StreamMode.SAFE:
            self.spill_to_disk(file_path, schema)
        else:
            raise ValueError(f'Invalid stream mode: {self._stream_mode}')
            
    # OPTIMIZE
    def spill_to_disk(self, file_path: Path, schema: pa.Schema):
        '''Spill in-memory buffer to buffer.arrow'''
        self._open_writers(file_path, schema)
        row = { name: [ streaming_data[name] for streaming_data in self._buffer ] for name in schema.names }  # make each column a list
        batch = pa.record_batch(row, schema=schema)
        self._ipc_writers[file_path].write_batch(batch)
        buffer_file = self._buffer_files[file_path]
        buffer_file.flush()  # flush python buffer to OS buffer
        os.fsync(buffer_file.fileno())  # flush OS buffer to disk
        # write_size = os.fstat(buffer_file.fileno()).st_size
        self._buffer.clear()    
            
    def read(self, file_path: Path) -> pa.Table:
        with open(file_path, "rb") as f:
            with pa.ipc.open_stream(f) as reader:
                table = reader.read_all()
                table = table.combine_chunks()
                return table

    def clear_disk(self, file_path: Path):
        '''Clear the buffer.arrow file'''
        self._close_writers(file_path)
        if file_path.exists():
            file_path.unlink()