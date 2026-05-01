from __future__ import annotations
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    import pyarrow as pa
    from lancedb import LanceDBConnection
    from lancedb.pydantic import LanceModel
    from lancedb.table import LanceTable
    from pfeed.data_models.base_data_model import BaseMetadataModel
    from pfeed._io.base_io import MetadataModelAsDict
    from pfeed._io.table_io import TablePath

from pfeed.utils.file_path import FilePath
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
    METADATA_FILENAME: str = "lancedb_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage
    TIMESTAMP_PRECISION = TimestampPrecision.NANOSECOND

    def __init__(
        self,
        storage_options: dict[str, Any] | None = None,
        connect_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        write_options: dict[str, Any] | None = None,
        filesystem: pa_fs.FileSystem=pa_fs.LocalFileSystem(),
        **kwargs: Any,  # for compatibility with other IO classes
    ):
        DatabaseIO.__init__(
            self, 
            storage_options=storage_options, 
            connect_options=connect_options, 
            read_options=read_options, 
            write_options=write_options,
        )
        TableIO.__init__(
            self, 
            storage_options=storage_options, 
            connect_options=connect_options, 
            read_options=read_options, 
            write_options=write_options,
            filesystem=filesystem,
        )

    def _open_connection(self, uri: str):
        self._conn_uri = uri
        self._conn: LanceDBConnection = lancedb.connect(
            uri, storage_options=self._storage_options, **self._connect_options
        )
    
    def _close_connection(self):
        if self._conn is not None:
            lancedb_conn = self._conn._conn
            if lancedb_conn.is_open():
                lancedb_conn.close()

    def exists(self, db_path: DBPath) -> bool:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return db_path.table_name in conn

    def is_empty(self, db_path: DBPath) -> bool:
        table = self.get_table(db_path)
        return table.count_rows() == 0

    def get_table(self, db_path: DBPath, **io_kwargs: Any) -> LanceTable:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return conn.open_table(db_path.table_name, storage_options=self._storage_options, **io_kwargs)

    def write(
        self,
        data: list[dict[str, Any]] | pa.Table,
        db_path: DBPath,
        delete_where: str | None = None,
        schema: pa.Schema | LanceModel | None = None,
        **io_kwargs: Any,
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
        io_kwargs = io_kwargs or self._write_options
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
        self, 
        db_path: DBPath, 
        **io_kwargs: Any
    ) -> pl.LazyFrame | None:
        io_kwargs = io_kwargs or self._read_options
        lf: pl.LazyFrame | None = None
        if self.exists(db_path):
            table = self.get_table(db_path, **io_kwargs)
            lf = table.to_polars()
        return lf
    
    def write_metadata(self, db_path: DBPath, metadata: BaseMetadataModel):
        return TableIO.write_metadata(
            self,
            table_path=FilePath(db_path.db_uri),
            metadata=metadata,
        )

    def read_metadata(
        self, 
        db_path: DBPath,
        max_retries: int=5,
        base_delay: float=0.1,
    ) -> dict[DBPath, MetadataModelAsDict]:
        table_path = FilePath(db_path.db_uri)
        metadata: dict[TablePath, MetadataModelAsDict] = TableIO.read_metadata(
            self,
            table_path=table_path,
            max_retries=max_retries,
            base_delay=base_delay,
        )
        return { db_path: metadata[table_path] } if metadata else {}