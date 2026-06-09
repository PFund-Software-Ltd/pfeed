# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    import polars as pl
    from deltalake import DeltaTable
    from lancedb.table import LanceTable

    from pfeed._io.base_io import MetadataDict
    from pfeed.data_handlers.base_data_handler import BaseDataMetadata

    Table: TypeAlias = DeltaTable | LanceTable

import random
import time
from abc import ABC, abstractmethod

import polars as pl
import pyarrow as pa
import pyarrow.fs as pa_fs

from pfeed._io.parquet_io import ParquetIO
from pfeed.utils.file_path import FilePath


class TablePath(FilePath):
    pass


class TableIO(ParquetIO, ABC):
    FILE_EXTENSION: str | None = None

    def exists(self, table_path: TablePath, *args: Any, **kwargs: Any) -> bool:
        file_info = self._filesystem.get_file_info(table_path.schemeless)
        return file_info.type == pa_fs.FileType.Directory

    @abstractmethod
    def is_empty(self, table_path: TablePath, *args: Any, **kwargs: Any) -> bool:
        pass

    @abstractmethod
    def get_table(self, table_path: TablePath, *args: Any, **kwargs: Any) -> Table:
        pass

    @abstractmethod
    def write(self, data: pa.Table, table_path: TablePath, *args: Any, **kwargs: Any):
        pass

    @abstractmethod
    def read(
        self, table_path: TablePath, *args: Any, **kwargs: Any
    ) -> pl.LazyFrame | None:
        pass

    # NOTE: delta-rs, lancedb doesn't support writing metadata, so create an empty df and use pyarrow to write metadata
    def write_metadata(self, table_path: TablePath, metadata: BaseDataMetadata):
        empty_df_with_metadata = pl.DataFrame()
        table = empty_df_with_metadata.to_arrow()
        table_with_metadata = super().write_metadata(table, metadata)
        metadata_file_path = table_path / self.METADATA_FILENAME
        self._write_pyarrow_table(table_with_metadata, metadata_file_path)

    def read_metadata(
        self,
        table_path: TablePath,
        max_retries: int = 5,
        base_delay: float = 0.1,
    ) -> dict[TablePath, MetadataDict]:
        """Read custom application metadata embedded in parquet schema.

        Handles race condition for table formats where metadata files may not exist immediately
        after table creation. This occurs when concurrent writers race:
        - Writer A creates the table structure
        - Writer B sees the table exists and tries to read metadata
        - Writer A hasn't finished writing the metadata file yet → FileNotFoundError

        Args:
            table_path: Path to the table.
            max_retries: Maximum number of retry attempts.
            base_delay: Initial delay in seconds; doubles each retry (exponential backoff).

        Returns:
            `{table_path: metadata}` if the sidecar is found, or `{}` if the table
            itself does not exist (no retry in that case).

        Raises:
            FileNotFoundError: The table exists but the sidecar metadata file did
                not appear within `max_retries` attempts.
        """
        if not self.exists(table_path):
            return {}

        metadata_file_path = table_path / self.METADATA_FILENAME
        for attempt in range(max_retries):
            try:
                file_metadata = super().read_metadata(file_paths=[metadata_file_path])
                if not file_metadata:
                    return {}
                else:
                    return {table_path: file_metadata[metadata_file_path]}
            except FileNotFoundError as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt) + random.uniform(0, base_delay)
                    time.sleep(delay)
                else:
                    raise e
        return {}
