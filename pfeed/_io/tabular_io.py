from __future__ import annotations
from typing import TYPE_CHECKING, Any
from types import ModuleType
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing.core import tDataFrame
    from pfeed.typing.literals import tDATA_TOOL

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetFile
import pandas as pd
try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None

from pfeed._io.base_io import BaseIO


class TabularIO(BaseIO):
    def __init__(
        self,
        filesystem: pa_fs.FileSystem,
        compression: str='snappy',
        storage_options: dict | None=None,
        use_deltalake: bool=False,
    ):
        super().__init__(filesystem, compression=compression, storage_options=storage_options)
        self._use_deltalake = use_deltalake
        if use_deltalake:
            assert DeltaTable is not None, 'deltalake is not installed'
    
    def _write_deltalake(self, table: pa.Table, file_path: str, metadata: dict | None=None):
        from deltalake import write_deltalake
        file_path_without_filename, filename = file_path.rsplit('/', 1)
        write_deltalake(file_path_without_filename, table, mode='overwrite', storage_options=self._storage_options)
        if metadata:
            # HACK: delta-rs doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
            empty_df_with_metadata = pd.DataFrame()
            table = pa.Table.from_pandas(empty_df_with_metadata, preserve_index=False)
            file_path = file_path_without_filename + '/.metadata/' + filename
            self._write_pyarrow_table(table, file_path, metadata=metadata)
    
    def _write_pyarrow_table(self, table: pa.Table, file_path: str, metadata: dict | None=None):
        file_path = file_path.replace('s3://', '')
        if metadata:
            schema = table.schema.with_metadata(metadata)
            table = table.replace_schema_metadata(schema.metadata)
        with self._filesystem.open_output_stream(file_path) as f:
            pq.write_table(table, f, compression=self._compression)
    
    def write(self, data: pd.DataFrame, file_path: str):
        self._mkdir(file_path)
        table = pa.Table.from_pandas(data, preserve_index=False)
        # NOTE: delta lake doesn't support writing empty df, so we need to write a parquet file for empty df
        if self._use_deltalake and not data.empty:
            self._write_deltalake(table, file_path)
        else:
            self._write_pyarrow_table(table, file_path)

    def read(
        self,
        file_paths: str | list[str],
        data_tool: tDATA_TOOL='polars',
        delta_version: int | None=None,
    ) -> tuple[tDataFrame | None, dict[str, Any]]:
        from pfeed._etl.base import get_data_tool
        
        if isinstance(file_paths, str):
            file_paths = [file_paths]
            
        data = None
        metadata = {}
        data_tool: ModuleType = get_data_tool(data_tool)
        if self._use_deltalake:
            file_dirs = [file_path.rsplit('/', 1)[0] for file_path in file_paths]
            delta_table_file_dirs = [file_dir for file_dir in file_dirs if DeltaTable.is_deltatable(file_dir, storage_options=self._storage_options)]
            # empty dfs were written as parquet files
            empty_parquet_file_dirs = [file_path.rsplit('/', 1)[0] for file_path in file_paths if self._exists(file_path)]
            if delta_table_file_dirs:
                data: tDataFrame = data_tool.read_delta(
                    delta_table_file_dirs,
                    storage_options=self._storage_options,
                    version=delta_version
                )
            metadata['missing_file_paths'] = list(set(file_dirs) - set(delta_table_file_dirs) - set(empty_parquet_file_dirs))
        else:
            if exists_file_paths := [file_path for file_path in file_paths if self._exists(file_path)]:
                non_empty_file_paths = [
                    file_path for file_path in exists_file_paths
                    if ParquetFile(file_path).metadata.num_rows > 0
                ]
                data: tDataFrame = data_tool.read_parquet(
                    non_empty_file_paths,
                    storage_options=self._storage_options,
                    filesystem=self._filesystem if not self.is_local_fs else None,
                )
            metadata['missing_file_paths'] = list(set(file_paths) - set(exists_file_paths))
        # REVIEW: parquet files metadata not in use for now
        # for file_path in exists_file_paths:
        #     file_path = file_path.replace('s3://', '')
        #     with self._filesystem.open_input_file(file_path) as f:
        #         parquet_file = pq.ParquetFile(f)
        #         parquet_file_metadata = parquet_file.schema.to_arrow_schema().metadata
        #         parquet_file_metadata = {k.decode(): v.decode() for k, v in parquet_file_metadata.items()}
        #         if parquet_file_metadata:
        #             metadata[file_path] = parquet_file_metadata
        return data, metadata
