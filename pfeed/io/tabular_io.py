from __future__ import annotations
from typing import TYPE_CHECKING
from types import ModuleType
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tDATA_TOOL

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from pfeed.io.base_io import BaseIO


class TabularIO(BaseIO):
    def __init__(
        self,
        file_system: pa_fs.FileSystem,
        compression: str='snappy',
        storage_options: dict | None=None,
        use_deltalake: bool=False,
    ):
        super().__init__(file_system, compression=compression, storage_options=storage_options)
        self._use_deltalake = use_deltalake
    
    def write(self, data: tDataFrame, file_path: str, metadata: dict | None=None):
        metadata = metadata or {}
        self._mkdir(file_path)
        if self._use_deltalake:
            import deltalake as dl
            # NOTE: writing metadata to commit_properties is NOT persistent, so don't use it
            file_path_without_filename = file_path.rsplit('/', 1)[0]
            dl.write_deltalake(file_path_without_filename, data, mode='overwrite', storage_options=self._storage_options)
            # HACK: delta-rs doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
            data = pd.DataFrame()
        table = pa.Table.from_pandas(data, preserve_index=False)
        schema = table.schema.with_metadata(metadata)
        table = table.replace_schema_metadata(schema.metadata)
        with self._file_system.open_output_stream(file_path) as f:
            pq.write_table(table, f, compression=self._compression)

    def read(
        self,
        file_path: str,
        data_tool: tDATA_TOOL='pandas',
        delta_version: int | None=None,
    ) -> tuple[tDataFrame | None, dict]:
        from pfeed.etl import get_data_tool
        metadata = {}
        if not self._exists(file_path):
            return None, metadata
        data_tool: ModuleType = get_data_tool(data_tool)
        if self._use_deltalake:
            from deltalake import DeltaTable
            file_path_without_filename = file_path.rsplit('/', 1)[0]
            if DeltaTable.is_deltatable(file_path_without_filename, storage_options=self._storage_options):
                dt = DeltaTable(
                    file_path_without_filename, 
                    storage_options=self._storage_options, 
                    version=delta_version
                )
                data: tDataFrame = data_tool.read_delta(dt)
            else:
                raise ValueError(f'{file_path_without_filename} is not a deltalake table')
        else:
            fs = self._file_system if not self.is_local_fs else None
            data: tDataFrame = data_tool.read_parquet(
                file_path, 
                file_system=fs,
                storage_options=self._storage_options,
            )
        with self._file_system.open_input_file(file_path) as f:
            parquet_file = pq.ParquetFile(f)
            metadata = parquet_file.schema.to_arrow_schema().metadata
            metadata = {k.decode(): v.decode() for k, v in metadata.items()}
        return data, metadata
