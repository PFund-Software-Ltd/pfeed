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
try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None

from pfeed.io.base_io import BaseIO


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
        assert DeltaTable is not None, 'deltalake is not installed'
    
    def write(self, data: pd.DataFrame, file_path: str):
        file_path_without_filename, filename = file_path.rsplit('/', 1)
        self._mkdir(file_path)
        table = pa.Table.from_pandas(data, preserve_index=False)
        if self._use_deltalake:
            import deltalake as dl
            # NOTE: writing metadata to commit_properties is NOT persistent, so don't use it
            dl.write_deltalake(file_path_without_filename, table, mode='overwrite', storage_options=self._storage_options)
            # REVIEW: ignore metadata for now
            # HACK: delta-rs doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
            # empty_df_with_metadata = pd.DataFrame()
            # table = pa.Table.from_pandas(empty_df_with_metadata, preserve_index=False)
            # file_path = file_path_without_filename + '/.metadata/' + filename
        else:
            file_path = file_path.replace('s3://', '')
            # REVIEW: ignore metadata for now
            # schema = table.schema.with_metadata(metadata)
            # table = table.replace_schema_metadata(schema.metadata)
            with self._filesystem.open_output_stream(file_path) as f:
                pq.write_table(table, f, compression=self._compression)

    def read(
        self,
        file_path: str,
        data_tool: tDATA_TOOL='polars',
        delta_version: int | None=None,
    ) -> tDataFrame | None:
        from pfeed.etl import get_data_tool
        metadata = {}
        file_path_without_filename, filename = file_path.rsplit('/', 1)
        if self._use_deltalake:
            is_exists = DeltaTable.is_deltatable(file_path_without_filename, storage_options=self._storage_options)
        else:
            is_exists = self._exists(file_path)
        if not is_exists:
            return None, metadata
        data_tool: ModuleType = get_data_tool(data_tool)
        if self._use_deltalake:
            if data_tool == 'polars':
                data: tDataFrame = data_tool.read_delta(
                    file_path_without_filename, 
                    storage_options=self._storage_options,
                    version=delta_version
                )
            else:
                dt = DeltaTable(
                    file_path_without_filename, 
                    storage_options=self._storage_options, 
                    version=delta_version
                )
                data: tDataFrame = data_tool.read_delta(dt)
            # REVIEW: ignore metadata for now
            # file_path = file_path_without_filename + '/.metadata/' + filename
        else:
            fs = self._filesystem if not self.is_local_fs else None
            data: tDataFrame = data_tool.read_parquet(
                file_path,
                filesystem=fs,
                storage_options=self._storage_options,
            )
        # REVIEW: ignore metadata for now
        # file_path = file_path.replace('s3://', '')
        # with self._filesystem.open_input_file(file_path) as f:
        #     parquet_file = pq.ParquetFile(f)
        #     metadata = parquet_file.schema.to_arrow_schema().metadata
        #     metadata = {k.decode(): v.decode() for k, v in metadata.items()}
        return data
