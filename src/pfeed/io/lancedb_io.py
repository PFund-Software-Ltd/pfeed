# pyright: reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnsafeMultipleInheritance=false, reportAttributeAccessIssue=false, reportUnnecessaryComparison=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pyarrow as pa
    from lancedb import LanceDBConnection
    from lancedb.pydantic import LanceModel
    from lancedb.table import LanceTable

    from pfeed.data_handlers.base_data_handler import BaseDataMetadata
    from pfeed.io.base_io import MetadataDict

import lancedb
import math
from lancedb.index import FTS, IvfPq
import polars as pl
import pyarrow.fs as pa_fs

from pfeed.enums import IOFormat, TimestampPrecision
from pfeed.io.database_io import DatabaseIO, DBPath
from pfeed.io.table_io import TableIO, TablePath


class LanceDBIO(DatabaseIO, TableIO):
    IO_FORMAT = IOFormat.LANCEDB
    SUPPORTS_PARALLEL_WRITES: bool = True
    METADATA_FILENAME: str = "lancedb_metadata.parquet"  # used by table format (e.g. Delta Lake) for metadata storage
    TIMESTAMP_PRECISION = TimestampPrecision.NANOSECOND
    DATE_FILTER_PREDICATE: str = "{date_col} >= cast('{start_date}' as timestamp) AND {date_col} <= cast('{end_date}' as timestamp)"

    def __init__(
        self,
        storage_options: dict[str, Any] | None = None,
        connect_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
        write_options: dict[str, Any] | None = None,
        filesystem: pa_fs.FileSystem | None = None,
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
        self._conn: LanceDBConnection = lancedb.connect(
            uri, storage_options=self._storage_options, **self._connect_options
        )
        self._conn_uri = uri

    def _close_connection(self):
        if self._conn is not None:
            lancedb_conn = self._conn._conn
            if lancedb_conn.is_open():
                _ = lancedb_conn.close()

    def exists(self, db_path: DBPath) -> bool:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return db_path.table_name in conn

    def is_empty(self, db_path: DBPath) -> bool:
        table = self.get_table(db_path)
        return table.count_rows() == 0

    def get_table(self, db_path: DBPath, **io_kwargs: Any) -> LanceTable:
        conn: LanceDBConnection = self.connect(db_path.db_uri)
        return conn.open_table(
            db_path.table_name, storage_options=self._storage_options, **io_kwargs
        )

    def write(
        self,
        data: list[dict[str, Any]] | pa.Table,
        db_path: DBPath,
        delete_where: str | None = None,
        schema: pa.Schema | LanceModel | None = None,
        merge_keys: list[str] | None = None,
        **io_kwargs: Any,
    ):
        """Append rows, replace a predicate, or atomically upsert by key.

        Args:
            data: Data to write as list of dicts or PyArrow table.
            db_path: Path to the LanceDB database.
            delete_where: SQL WHERE clause to delete existing rows before inserting.
                If None, data is appended (may cause duplicates if called twice).
                If provided, matching rows are deleted first, then new data is inserted,
                enabling idempotent writes for a given predicate.
                Example: "date >= '2024-01-15' AND date <= '2024-01-20'"
            schema: Optional schema for table creation.
            merge_keys: Columns identifying rows to replace in one atomic commit.
                Cannot be combined with delete_where.
        """
        io_kwargs = io_kwargs or self._write_options
        if merge_keys is not None and (not merge_keys or delete_where):
            raise ValueError(
                "merge_keys must be nonempty and cannot be combined with delete_where"
            )
        conn: LanceDBConnection = self.connect(db_path.db_uri)

        # Safety net for direct callers: enforce IO-level schema policy. Callers
        # that need post-conform metadata (e.g. start/end ts for delete_where)
        # should call self.conform(data) explicitly before reaching here; doing
        # so makes this call a no-op since conform() is idempotent.
        # Skip for list[dict] inputs — narwhals does not accept them and there
        # is no typed datetime precision to enforce at that stage.
        if not isinstance(data, list):
            data = self.conform(data)

        if not self.exists(db_path):
            try:
                conn.create_table(
                    db_path.table_name, data, schema=schema, mode="create", **io_kwargs
                )
                return
            except ValueError as exc:
                # A concurrent writer may have won table creation. Never
                # overwrite its rows; continue with the normal append/upsert.
                if "already exists" not in str(exc).lower():
                    raise
        table = self.get_table(db_path)
        if merge_keys is not None:
            # One commit: no delete/add gap or restore that could revert
            # another writer's successful commit.
            (
                table.merge_insert(merge_keys)
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(data)
            )
            return
        if not delete_where:
            # add() is atomic; a failed append must not restore an older version
            # over a concurrent writer's successful commit.
            table.add(data)
            return
        # LanceDB has no BEGIN/COMMIT, but tables are versioned: snapshot the
        # version before the delete+add, and on failure restore() rolls the
        # table back to that snapshot so the pair fails atomically (otherwise
        # a successful delete + failed add would leave a half-destroyed table).
        version_before = table.version
        try:
            if delete_where:
                _ = table.delete(where=delete_where)
            _ = table.add(data)
        except Exception as exc:
            try:
                table.restore(version_before)
            except Exception as restore_exc:
                raise Exception(
                    f"Failed to restore table to version {version_before} after write failure ({db_path=}): {restore_exc}"
                ) from exc
            raise Exception(
                f"Failed to write data (type={type(data)}) ({db_path=}); rolled back to version {version_before}: {exc}"
            ) from exc

    def read(
        self,
        db_path: DBPath,
        where: str | None = None,
        columns: list[str] | None = None,
        **io_kwargs: Any,
    ) -> pl.LazyFrame | None:
        """Read a table from LanceDB, optionally filtered and projected.

        Args:
            db_path: Path to the LanceDB database.
            where: SQL-style WHERE clause to filter rows on read. Same trust
                model as write()'s delete_where — callers construct this from
                internal, non-user-controlled values.
                Example: "date >= '2024-01-01'".
            columns: Columns to return; None returns every column. Skipping
                wide columns such as vectors is much cheaper than reading them.
        """
        io_kwargs = io_kwargs or self._read_options
        lf: pl.LazyFrame | None = None
        if self.exists(db_path):
            table = self.get_table(db_path, **io_kwargs)
            query = table.search()
            if where:
                query = query.where(where)
            if columns:
                query = query.select(columns)
            # LanceDB returns LazyFrame from to_polars() but an eager DataFrame
            # from search()...to_polars(); normalize to LazyFrame either way.
            result = query.to_polars() if where or columns else table.to_polars()
            lf = result.lazy() if isinstance(result, pl.DataFrame) else result
        return lf

    # Rows below this count are scanned exactly; an approximate vector index
    # only pays off once brute force stops being instant.
    VECTOR_INDEX_MIN_ROWS: int = 50_000
    # IVF-PQ training needs this many rows; Lance refuses to build below it.
    _VECTOR_INDEX_TRAINING_ROWS: int = 256

    RRF_K = 60

    def search(
        self,
        db_path: DBPath,
        query_vector: Sequence[float] | None = None,
        query_text: str | None = None,
        limit: int = 10,
        where: str | None = None,
        vector_column: str = "vector",
        text_column: str = "text",
        min_similarity: float | None = None,
        nprobes: int | None = None,
        refine_factor: int | None = None,
        **io_kwargs: Any,
    ) -> pl.LazyFrame | None:
        """Vector, full-text, or hybrid (reciprocal-rank-fused) search.

        Hybrid runs both legs and fuses them here rather than in LanceDB, so
        the vector leg can be floored before fusion: rank fusion has no notion
        of "nothing matched", a floor does.

        Args:
            min_similarity: Drop vector hits whose cosine similarity is below
                this. Full-text hits are kept regardless; an exact term match
                is relevant however the embedding scores it.
            nprobes: Partitions probed when a vector index exists; higher is
                slower and more accurate. Ignored on unindexed tables.
            refine_factor: Re-rank this many times ``limit`` candidates with
                exact distances after the approximate index pass.
        """
        if query_vector is None and query_text is None:
            raise ValueError("search requires query_vector, query_text, or both")
        if limit <= 0:
            raise ValueError("limit must be positive")
        if query_vector is not None:
            query_vector = list(query_vector)
            if (
                not query_vector
                or not all(math.isfinite(v) for v in query_vector)
                or not any(query_vector)
            ):
                raise ValueError(
                    "cosine search requires a finite, nonzero query vector"
                )
        if not self.exists(db_path):
            return None
        table = self.get_table(db_path, **(io_kwargs or self._read_options))

        legs: list[pl.DataFrame] = []
        if query_vector is not None:
            query = table.search(
                query_vector, vector_column_name=vector_column, query_type="vector"
            ).distance_type("cosine")
            if where:
                query = query.where(where, prefilter=True)
            if nprobes is not None:
                query = query.nprobes(nprobes)
            if refine_factor is not None:
                query = query.refine_factor(refine_factor)
            df = self._collect(query.with_row_id(True).limit(limit))
            # cosine distance in [0, 2]; 1 - distance is the cosine similarity
            df = df.with_columns((1.0 - pl.col("_distance")).alias("score")).drop("_distance")
            if min_similarity is not None:
                df = df.filter(pl.col("score") >= min_similarity)
            legs.append(df)
        if query_text is not None:
            self._ensure_fts_index(table, text_column)
            query = table.search(query_text, query_type="fts", fts_columns=text_column)
            if where:
                query = query.where(where, prefilter=True)
            df = self._collect(query.with_row_id(True).limit(limit))
            legs.append(df.rename({"_score": "score"}))

        if len(legs) == 1:
            df = legs[0]
        else:
            df = self._fuse(legs)
        return df.drop("_rowid").sort("score", descending=True).head(limit).lazy()

    @staticmethod
    def _collect(query: Any) -> pl.DataFrame:
        df = query.to_polars()
        return df.collect() if isinstance(df, pl.LazyFrame) else df

    @classmethod
    def _fuse(cls, legs: list[pl.DataFrame]) -> pl.DataFrame:
        """Reciprocal rank fusion: each leg contributes 1 / (k + rank) per row."""
        ranked = [
            leg.sort("score", descending=True)
            .with_row_index("_rank")
            .with_columns((1.0 / (cls.RRF_K + pl.col("_rank") + 1)).alias("score"))
            .drop("_rank")
            for leg in legs
        ]
        columns = [c for c in ranked[0].columns if c not in ("_rowid", "score")]
        return (
            pl.concat(ranked, how="diagonal")
            .group_by("_rowid")
            .agg([pl.col("score").sum(), *[pl.col(c).first() for c in columns]])
        )

    def create_search_index(
        self,
        db_path: DBPath,
        vector_column: str = "vector",
        text_column: str = "text",
        min_rows: int | None = None,
        **io_kwargs: Any,
    ) -> None:
        """Build the full-text index and, once the table holds at least
        ``min_rows`` rows (default ``VECTOR_INDEX_MIN_ROWS``), an IVF-PQ cosine
        index on the vector column.

        Lance indexes are incremental: rows added after the build are still
        searched, by brute force, until the next call folds them in.
        """
        if not self.exists(db_path):
            return
        table = self.get_table(db_path, **io_kwargs)
        self._ensure_fts_index(table, text_column)
        threshold = max(
            min_rows if min_rows is not None else self.VECTOR_INDEX_MIN_ROWS,
            self._VECTOR_INDEX_TRAINING_ROWS,
        )
        has_vector_index = any(
            vector_column in index.columns
            and index.index_type.upper().startswith("IVF")
            for index in table.list_indices()
        )
        if table.count_rows() >= threshold and not has_vector_index:
            table.create_index(
                vector_column, config=IvfPq(distance_type="cosine"), replace=True
            )
        table.optimize()

    @staticmethod
    def _ensure_fts_index(table: LanceTable, text_column: str) -> None:
        indexed_columns = {
            column
            for index in table.list_indices()
            if index.index_type == "FTS"
            for column in index.columns
        }
        if text_column not in indexed_columns:
            table.create_index(text_column, config=FTS(), replace=True)

    def write_metadata(self, db_path: DBPath, metadata: BaseDataMetadata):
        return TableIO.write_metadata(
            self,
            table_path=TablePath(db_path.db_uri),
            metadata=metadata,
        )

    def read_metadata(
        self,
        db_path: DBPath,
        max_retries: int = 5,
        base_delay: float = 0.1,
    ) -> dict[DBPath, MetadataDict]:
        table_path = TablePath(db_path.db_uri)
        metadata: dict[TablePath, MetadataDict] = TableIO.read_metadata(
            self,
            table_path=table_path,
            max_retries=max_retries,
            base_delay=base_delay,
        )
        return {db_path: metadata[table_path]} if metadata else {}
