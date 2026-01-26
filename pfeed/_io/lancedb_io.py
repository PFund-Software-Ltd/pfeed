from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pyarrow as pa
    from lancedb import LanceDBConnection
    from lancedb.pydantic import LanceModel
    from lancedb.table import LanceTable

import polars as pl
import pyarrow.fs as pa_fs
import lancedb

from pfeed.enums import TimestampPrecision
from pfeed._io.table_io import TableIO
from pfeed._io.database_io import DatabaseIO, DBPath


class LanceDBIO(DatabaseIO, TableIO):
    # TODO: support streaming
    # SUPPORTS_STREAMING: bool = True
    SUPPORTS_PARALLEL_WRITES: bool = True
    FILE_EXTENSION = ".lancedb"
    METADATA_FILENAME: str = "lancedb_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage
    TIMESTAMP_PRECISION = TimestampPrecision.NANOSECOND

    def __init__(
        self,
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(),
        storage_options: dict | None = None,
        io_options: dict | None = None,
    ):
        DatabaseIO.__init__(self, storage_options=storage_options, io_options=io_options)
        TableIO.__init__(self, filesystem=filesystem, storage_options=storage_options, io_options=io_options)

    def _open_connection(self, uri: str):
        self._conn_uri = uri
        self._conn: LanceDBConnection = lancedb.connect(
            uri, storage_options=self._storage_options, **self._io_options
        )
    
    def _close_connection(self):
        if self._conn:
            self._conn.close()

    def exists(self, db_path: DBPath) -> bool:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return db_path.table_name in conn

    def is_empty(self, db_path: DBPath) -> bool:
        table = self.get_table(db_path)
        return table.count_rows() == 0

    # FIXME: remove io_kwargs and pass in args from lancedb explicitly
    def get_table(self, db_path: DBPath) -> LanceTable:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return conn.open_table(db_path.table_name, storage_options=self._storage_options)

    def write(
        self,
        data: list[dict] | pa.Table,
        db_path: DBPath,
        delete_where: str | None = None,
        schema: pa.Schema | LanceModel | None = None,
        **io_kwargs,
    ):
        """Write data to LanceDB table.

        Args:
            data: Data to write as list of dicts or PyArrow table.
            db_path: Path to the LanceDB database.
            delete_where: Optional filter to replace only matching rows.
                If None, data is appended. If provided, matching rows are deleted
                before new data is inserted (e.g., "date >= '2024-01-15' AND date <= '2024-01-20'").
            schema: Optional schema for table creation.
        """
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        if not self.exists(db_path):
            table = conn.create_table(db_path.table_name, data, schema=schema, mode="overwrite", **io_kwargs)
        else:
            table = self.get_table(db_path)
            # Delete matching rows, then add new data
            if delete_where:
                table.delete(where=delete_where)
            table.add(data)

    def read(
        self, db_path: DBPath
    ) -> pl.LazyFrame | None:
        lf: pl.LazyFrame | None = None
        if self.exists(db_path):
            table = self.get_table(db_path)
            lf = table.to_polars()
        return lf
