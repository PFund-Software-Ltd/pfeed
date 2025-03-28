from __future__ import annotations
from typing import TYPE_CHECKING, Any
from types import ModuleType
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs
    from pfeed.typing import GenericFrame
    from pfeed.typing import tDATA_TOOL

import pyarrow as pa
import pyarrow.parquet as pq
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
        # HACK: delta-rs doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
        if metadata:
            empty_df_with_metadata = pd.DataFrame()
            table = pa.Table.from_pandas(empty_df_with_metadata, preserve_index=False)
            file_path = file_path_without_filename + '/.metadata/' + filename
            self._write_pyarrow_table(table, file_path, metadata=metadata)
    
    def _write_pyarrow_table(self, table: pa.Table, file_path: str, metadata: dict | None=None):
        file_path = file_path.replace('s3://', '')
        if metadata:
            metadata = {str(k).encode(): str(v).encode() for k, v in metadata.items()}
            schema = table.schema.with_metadata(metadata)
            table = table.replace_schema_metadata(schema.metadata)
        with self._filesystem.open_output_stream(file_path) as f:
            pq.write_table(table, f, compression=self._compression)
            
    def _is_empty_parquet_file(self, file_path: str) -> bool:
        return self._exists(file_path) and pq.read_metadata(file_path).num_rows == 0
    
    def _find_deltalake_metadata_file_paths(self, file_paths: list[str], delta_table_file_dirs: list[str]) -> list[str]:
        metadata_file_paths: list[str] = []
        for file_path in file_paths:
            file_dir, filename = file_path.rsplit('/', 1)
            metadata_file_path = file_dir + '/.metadata/' + filename
            is_delta_table_metadata = file_dir in delta_table_file_dirs and self._is_empty_parquet_file(metadata_file_path)
            is_empty_parquet_file_metadata = self._is_empty_parquet_file(file_path)
            if is_delta_table_metadata:
                metadata_file_paths.append(metadata_file_path)
            elif is_empty_parquet_file_metadata:
                metadata_file_paths.append(file_path)
        return metadata_file_paths
    
    def write(self, df: pd.DataFrame, file_path: str, metadata: dict | None=None):
        self._mkdir(file_path)
        table = pa.Table.from_pandas(df, preserve_index=False)
        # NOTE: delta lake doesn't support writing empty df, so we need to write a parquet file for empty df
        if self._use_deltalake and not df.empty:
            self._write_deltalake(table, file_path, metadata=metadata)
        else:
            self._write_pyarrow_table(table, file_path, metadata=metadata)

    def read(
        self,
        file_paths: list[str],
        data_tool: tDATA_TOOL='polars',
        delta_version: int | None=None,
    ) -> tuple[GenericFrame | None, dict[str, Any]]:
        from pfeed._etl.base import get_data_tool
        
        df: GenericFrame | None = None
        metadata: dict[str, Any] = {}
        data_tool: ModuleType = get_data_tool(data_tool)
        if self._use_deltalake:
            file_dirs = [file_path.rsplit('/', 1)[0] for file_path in file_paths]
            delta_table_file_dirs = [file_dir for file_dir in file_dirs if DeltaTable.is_deltatable(file_dir, storage_options=self._storage_options)]
            # empty dfs were written as parquet files
            empty_parquet_file_dirs = [file_path.rsplit('/', 1)[0] for file_path in file_paths if self._is_empty_parquet_file(file_path)]
            if delta_table_file_dirs:
                df: GenericFrame = data_tool.read_delta(
                    delta_table_file_dirs,
                    storage_options=self._storage_options,
                    version=delta_version
                )
            metadata_file_paths = self._find_deltalake_metadata_file_paths(file_paths, delta_table_file_dirs)
            metadata['file_metadata'] = self._read_pyarrow_table_metadata(metadata_file_paths)
            metadata['missing_file_paths'] = list(set(file_dirs) - set(delta_table_file_dirs) - set(empty_parquet_file_dirs))
        else:
            exists_file_paths = [file_path for file_path in file_paths if self._exists(file_path)]
            non_empty_file_paths = [file_path for file_path in exists_file_paths if not self._is_empty_parquet_file(file_path)]
            if non_empty_file_paths:
                df: GenericFrame = data_tool.read_parquet(
                    non_empty_file_paths,
                    storage_options=self._storage_options,
                    filesystem=self._filesystem if not self.is_local_fs else None,
                )
            metadata['file_metadata'] = self._read_pyarrow_table_metadata(exists_file_paths)
            metadata['missing_file_paths'] = list(set(file_paths) - set(exists_file_paths))
        return df, metadata
            
    def _read_pyarrow_table_metadata(self, file_paths: list[str]) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for file_path in file_paths:
            file_path = file_path.replace('s3://', '')
            with self._filesystem.open_input_file(file_path) as f:
                parquet_file = pq.ParquetFile(f)
                parquet_file_metadata = parquet_file.schema.to_arrow_schema().metadata
                parquet_file_metadata = {k.decode(): v.decode() for k, v in parquet_file_metadata.items()}
                if parquet_file_metadata:
                    metadata[file_path] = parquet_file_metadata
        return metadata
