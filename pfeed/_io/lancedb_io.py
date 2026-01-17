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
import pyarrow.fs as pa_fs
import lancedb

from pfeed._io.table_io import TableIO
from pfeed._io.database_io import DatabaseIO


class LanceDBIO(DatabaseIO, TableIO):
    # TODO: support streaming
    # SUPPORTS_STREAMING: bool = True
    FILE_EXTENSION = ".lancedb"
    METADATA_FILENAME: str = "lancedb_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage

    def __init__(
        self,
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(),
        storage_options: dict | None = None,
        io_options: dict | None = None,
    ):
        DatabaseIO.__init__(self, storage_options=storage_options, io_options=io_options)
        TableIO.__init__(self, filesystem=filesystem, storage_options=storage_options, io_options=io_options)

    # no URI for lancedb, just use the table path
    def _create_uri(self, *args, **kwargs) -> str:
        pass

    def _open_connection(self, table_path: FilePath):
        self._conn_uri = str(table_path)
        self._conn: LanceDBConnection = lancedb.connect(
            self._conn_uri, storage_options=self._storage_options, **self._io_options
        )
    
    def _close_connection(self):
        self._conn.close()

    def exists(self, table_path: FilePath, table_name: str) -> bool:
        conn = self._connect(table_path)
        return table_name in conn

    def is_empty(self, table_path: FilePath, table_name: str) -> bool:
        table = self.get_table(table_path, table_name)
        return table.count_rows() == 0

    # FIXME: remove io_kwargs and pass in args from lancedb explicitly
    def get_table(self, table_path: FilePath, table_name: str) -> LanceTable:
        conn = self._connect(table_path)
        return conn.open_table(table_name, storage_options=self._storage_options)

    def write(
        self,
        data: list[dict] | pa.Table,
        table_path: FilePath,
        table_name: str,
        metadata: BaseMetadataModel | None = None,
        where: str | None = None,
        schema: pa.Schema | LanceModel | None = None,
        **io_kwargs,
    ):
        """Write data to LanceDB table.

        Args:
            data: Data to write as list of dicts or PyArrow table.
            table_path: Path to the LanceDB database.
            table_name: Name of the table to write to.
            metadata: Optional metadata to store alongside the data.
            where: Optional filter to replace only matching rows.
                If None, data is appended. If provided, matching rows are deleted
                before new data is inserted (e.g., "date >= '2024-01-15' AND date <= '2024-01-20'").
            schema: Optional schema for table creation.
        """
        conn = self._connect(table_path)
        if not self.exists(table_path, table_name):
            table = conn.create_table(table_name, data, schema=schema, mode="overwrite", **io_kwargs)
        else:
            table = self.get_table(table_path, table_name)
            # Delete matching rows, then add new data
            if where:
                table.delete(where=where)
            table.add(data)
        if metadata:
            self.write_metadata(table_path, metadata)

    def read(
        self, table_path: FilePath, table_name: str
    ) -> pl.LazyFrame | None:
        lf: pl.LazyFrame | None = None
        if self.exists(table_path, table_name):
            table = self.get_table(table_path, table_name)
            lf = table.to_polars()
        return lf
