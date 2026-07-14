# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyarrow.parquet import FileMetaData as PyArrowParquetFileMetaData

    from pfeed._io.base_io import MetadataDict
    from pfeed.data_handlers.base_data_handler import BaseDataMetadata
    from pfeed.utils.file_path import FilePath

import json

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from pfeed._io.file_io import FileIO


class ParquetIO(FileIO):
    # can't write to the SAME file in parallel
    SUPPORTS_PARALLEL_WRITES: bool = False
    FILE_EXTENSION: str = ".parquet"

    def exists(self, file_path: FilePath) -> bool:
        """Check if a valid parquet file exists at this path.

        Returns:
            True if file exists and is a valid parquet file
            False if file doesn't exist or is not valid parquet
        """
        if not FileIO.exists(self, file_path):
            return False

        try:
            # Try to read parquet metadata to validate format
            self._get_pyarrow_file_metadata(file_path)
            return True
        except (pa.ArrowInvalid, Exception):
            # File exists but is not valid parquet
            return False

    def _get_pyarrow_file_metadata(
        self, file_path: FilePath
    ) -> PyArrowParquetFileMetaData:
        """Get file metadata created by pyarrow (e.g. rows, schema, row groups, etc.)."""
        return pq.read_metadata(file_path.schemeless, filesystem=self._filesystem)

    def is_empty(self, file_path: FilePath) -> bool:
        return self._get_pyarrow_file_metadata(file_path).num_rows == 0

    def _write_pyarrow_table(
        self, table: pa.Table, file_path: FilePath, **io_kwargs: Any
    ):
        self._mkdir(file_path)
        with self._filesystem.open_output_stream(file_path.schemeless) as f:
            pq.write_table(table, f, compression=self._compression, **io_kwargs)

    def write(self, data: pa.Table, file_path: FilePath, **io_kwargs: Any):
        io_kwargs = io_kwargs or self._write_options
        self._write_pyarrow_table(data, file_path, **io_kwargs)

    def read(self, file_paths: list[FilePath], **io_kwargs: Any) -> pl.LazyFrame | None:
        io_kwargs = io_kwargs or self._read_options
        lf: pl.LazyFrame | None = None
        non_empty_file_paths = [
            str(file_path)
            for file_path in file_paths
            if self.exists(file_path) and not self.is_empty(file_path)
        ]
        if non_empty_file_paths:
            lf = pl.scan_parquet(
                non_empty_file_paths, storage_options=self._storage_options, **io_kwargs
            )
        return lf

    def write_metadata(self, data: pa.Table, metadata: BaseDataMetadata) -> pa.Table:
        """This only writes metadata to the table schema, not to the file.
        You must call _write_pyarrow_table to actually write the metadata to the file.
        """
        metadata_json = json.dumps(
            metadata.model_dump(mode="json", fallback=str) if metadata else {}
        )
        schema = data.schema.with_metadata({b"metadata_json": metadata_json})
        return data.replace_schema_metadata(schema.metadata)

    def read_metadata(self, file_paths: list[FilePath]) -> dict[FilePath, MetadataDict]:
        metadata: dict[FilePath, MetadataDict] = {}

        for file_path in file_paths:
            if not FileIO.exists(self, file_path):
                continue
            with self._filesystem.open_input_file(file_path.schemeless) as f:
                parquet_file = pq.ParquetFile(f)
                parquet_file_metadata = parquet_file.schema.to_arrow_schema().metadata
                if b"metadata_json" in parquet_file_metadata:
                    metadata_json = parquet_file_metadata[b"metadata_json"]
                    metadata[file_path] = json.loads(metadata_json)
        return metadata
