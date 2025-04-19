from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow.fs as pa_fs

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import polars as pl
try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None

from pfeed._io.base_io import BaseIO
from pfeed.storages.delta_lake_storage_mixin import DeltaLakeStorageMixin


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
    
    def _write_deltalake_metadata(self, file_path: str, metadata: dict):
        # HACK: delta-rs doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
        empty_df_with_metadata = pd.DataFrame()
        table = pa.Table.from_pandas(empty_df_with_metadata, preserve_index=False)
        self._write_pyarrow_table(table, file_path + '/' + DeltaLakeStorageMixin.metadata_filename, metadata=metadata)
    
    def _write_deltalake(
        self, 
        table: pa.Table, 
        file_path: str, 
        metadata: dict,
        delta_partition_by: list[str] | None=None,
    ):
        from deltalake import write_deltalake
        is_empty_table = (table.num_rows == 0)
        if not is_empty_table:
            write_deltalake(
                file_path, 
                table, 
                mode='append',
                storage_options=self._storage_options,
                partition_by=delta_partition_by,
            )
        self._write_deltalake_metadata(file_path, metadata)

    def _write_pyarrow_table(self, table: pa.Table, file_path: str, metadata: dict | None=None):
        file_path = file_path.replace('s3://', '')
        metadata = metadata or {}
        metadata_json = json.dumps(metadata, default=str).encode()
        schema = table.schema.with_metadata({b'metadata_json': metadata_json})
        table = table.replace_schema_metadata(schema.metadata)
        with self._filesystem.open_output_stream(file_path) as f:
            pq.write_table(table, f, compression=self._compression)
            
    def _is_empty_parquet_file(self, file_path: str) -> bool:
        return self._exists(file_path) and pq.read_metadata(file_path).num_rows == 0
    
    def write(
        self, 
        file_path: str, 
        df: pd.DataFrame, 
        metadata: dict,
        delta_partition_by: list[str] | None=None,
    ):
        self._mkdir(file_path)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if self._use_deltalake:
            self._write_deltalake(
                table, 
                file_path, 
                metadata=metadata, 
                delta_partition_by=delta_partition_by,
            )
        else:
            self._write_pyarrow_table(table, file_path, metadata=metadata)

    def read(
        self,
        file_paths: list[str],
        delta_version: int | None=None,
    ) -> tuple[pl.LazyFrame | None, dict[str, Any]]:
        lf: pl.LazyFrame | None = None
        metadata: dict[str, Any] = {}
        if self._use_deltalake:
            exists_file_paths = [file_path for file_path in file_paths if self._exists(file_path + '/' + DeltaLakeStorageMixin.metadata_filename)]
            non_empty_file_paths = [file_path for file_path in exists_file_paths if DeltaTable.is_deltatable(file_path, storage_options=self._storage_options)]
            if non_empty_file_paths:
                assert len(non_empty_file_paths) == 1, f'Expected only one file path for deltalake, got {len(non_empty_file_paths)}'
                dt = DeltaTable(
                    non_empty_file_paths[0],
                    version=delta_version,
                    storage_options=self._storage_options,
                )
                lf = pl.scan_delta(dt, use_pyarrow=True)
            metadata['file_metadata'] = self._read_pyarrow_table_metadata(
                [file_path + '/' + DeltaLakeStorageMixin.metadata_filename for file_path in exists_file_paths]
            )
        else:
            exists_file_paths = [file_path for file_path in file_paths if self._exists(file_path)]
            non_empty_file_paths = [file_path for file_path in exists_file_paths if not self._is_empty_parquet_file(file_path)]
            if non_empty_file_paths:
                lf = pl.scan_parquet(
                    non_empty_file_paths,
                    storage_options=self._storage_options,
                )
            metadata['file_metadata'] = self._read_pyarrow_table_metadata(exists_file_paths)
        metadata['missing_file_paths'] = list(set(file_paths) - set(exists_file_paths))
        return lf, metadata
    
    def _read_pyarrow_table_metadata(self, file_paths: list[str]) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for file_path in file_paths:
            file_path = file_path.replace('s3://', '')
            with self._filesystem.open_input_file(file_path) as f:
                parquet_file = pq.ParquetFile(f)
                parquet_file_metadata = parquet_file.schema.to_arrow_schema().metadata
                if b'metadata_json' in parquet_file_metadata:
                    metadata_json = parquet_file_metadata[b'metadata_json']
                    metadata[file_path] = json.loads(metadata_json.decode())
        return metadata
