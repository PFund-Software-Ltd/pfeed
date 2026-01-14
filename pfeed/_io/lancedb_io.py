from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from lancedb import LanceDBConnection
    from lancedb.pydantic import LanceModel
    from lancedb.table import LanceTable
    from pfeed.utils.file_path import FilePath
    from pfeed.data_models.base_data_model import BaseMetadataModel

import polars as pl
import lancedb

from pfeed._io.table_io import TableIO


class LanceDBIO(TableIO):
    # TODO: support streaming
    # SUPPORTS_STREAMING: bool = True
    FILE_EXTENSION = ".lance"
    METADATA_FILENAME: str = "lancedb_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage

    def _connect(self, file_path: FilePath) -> LanceDBConnection:
        return lancedb.connect(
            str(file_path), storage_options=self._storage_options, **self._kwargs
        )

    def exists(self, file_path: FilePath, table_name: str) -> bool:
        db = self._connect(file_path)
        return table_name in db

    def is_empty(self, file_path: FilePath, table_name: str) -> bool:
        table = self.get_table(file_path, table_name)
        return table.count_rows() == 0

    def get_table(self, file_path: FilePath, table_name: str) -> LanceTable:
        db = self._connect(file_path)
        return db.open_table(table_name)

    def write(
        self,
        data: list[dict] | pa.Table,
        file_path: FilePath,
        table_name: str,
        metadata: BaseMetadataModel | None = None,
        where: str | None = None,
        schema: pa.Schema | LanceModel | None = None,
    ):
        """Write data to LanceDB table.

        Args:
            data: Data to write as list of dicts or PyArrow table.
            file_path: Path to the LanceDB database.
            table_name: Name of the table to write to.
            metadata: Optional metadata to store alongside the data.
            where: Optional filter to replace only matching rows.
                If None, data is appended. If provided, matching rows are deleted
                before new data is inserted (e.g., "date >= '2024-01-15' AND date <= '2024-01-20'").
            schema: Optional schema for table creation.
        """
        db = self._connect(file_path)
        if not self.exists(file_path, table_name):
            table = db.create_table(table_name, data, schema=schema, mode="overwrite")
        else:
            table = self.get_table(file_path, table_name)
            # Delete matching rows, then add new data
            if where:
                table.delete(where=where)
            table.add(data)
        if metadata:
            self.write_metadata(file_path, metadata)

    def read(
        self, file_paths: FilePath | list[FilePath], table_name: str
    ) -> pl.LazyFrame | None:
        if isinstance(file_paths, list):
            assert len(file_paths) == 1, "lancedb should have exactly one file path"
            table_path = file_paths[0]
        else:
            table_path = file_paths
        lf: pl.LazyFrame | None = None
        if self.exists(table_path, table_name):
            table = self.get_table(table_path, table_name)
            lf = table.to_polars()
        return lf
