from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, assert_never, cast

if TYPE_CHECKING:
    from collections.abc import Sequence
    from sqlite3 import Connection as SQLiteConnection
    from uuid import UUID

    from narwhals.typing import IntoFrame

    from pfeed.io.database_io import DatabaseIO
    from pfeed.sinks.base_sink import BaseSink
    from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
    from pfeed.sources.alphafund.channel_data_model import (
        AlphaFundChannelDataModel,
    )
    from pfeed.sources.alphafund.chat_data_model import AlphaFundChatDataModel
    from pfeed.sources.alphafund.embedding_data_model import (
        AlphaFundEmbeddingDataModel,
    )
    from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel
    from pfeed.sources.alphafund.message_data_model import (
        AlphaFundMessageDataModel,
    )
    from pfeed.storages.database_storage import DatabaseURI

    AlphaFundSQLDataModel: TypeAlias = (
        AlphaFundDataModel
        | AlphaFundAgentDataModel
        | AlphaFundChannelDataModel
        | AlphaFundChatDataModel
        | AlphaFundMessageDataModel
        | AlphaFundEmbeddingDataModel
    )

import polars as pl

from pfeed._etl.base import convert_dataframe
from pfeed.data_handlers.base_data_handler import BaseDataHandler, BaseDataMetadata
from pfeed.enums import DataLayer, DataSource, DataTool, IOFormat
from pfeed.io.database_io import DBPath
from pfeed.io.table_io import TablePath
from pfeed.utils.file_path import FilePath


# TODO: add agent's metadata?
class AlphaFundDataHandler(BaseDataHandler):
    """Persist AlphaFund entities with strict create/read/update semantics."""

    _data_model: AlphaFundSQLDataModel
    Metadata: ClassVar[type[BaseDataMetadata]] = BaseDataMetadata

    def __init__(
        self,
        data_path: FilePath | DatabaseURI,
        data_layer: DataLayer,
        data_domain: str,
        data_model: AlphaFundSQLDataModel,
        io: DatabaseIO,
        sink: BaseSink | None = None,
    ):
        if not io.is_database_io(strict=False):
            raise TypeError(f"{self.__class__.__name__} requires database IO")
        self._fund_id = data_model.fund_id
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            data_model=data_model,
            io=io,
            sink=sink,
        )

    def write_batch(self, data: IntoFrame, *args: Any, **kwargs: Any) -> None:
        self._check_io_supports_model()
        frame = cast(pl.LazyFrame, convert_dataframe(data, DataTool.polars))
        frame = self._validate_schema(frame)
        rows = frame.collect()
        if rows.height == 0:
            raise ValueError("An AlphaFund write must contain at least one row")
        if rows.height != 1 and not self._is_batch_model():
            raise ValueError(
                "An AlphaFund create or update operation must contain exactly one row"
            )
        if self._fund_id is not None and (
            rows["fund_id"].null_count()
            or (rows["fund_id"] != str(self._fund_id)).any()
        ):
            raise ValueError("Rows must belong to the fund bound to the data model")

        try:
            operation = self._data_model.op
        except AttributeError as exc:
            raise ValueError(
                "The data model operation must be set before writing"
            ) from exc

        if operation == "read":
            raise ValueError("A read data model cannot be written")
        if operation == "update":
            self._update_batch(rows)
            return
        if operation != "create":
            raise ValueError(f"Unsupported AlphaFund operation: {operation!r}")

        assert self._db_path is not None
        io_format = self.io.IO_FORMAT
        if io_format == IOFormat.SQLITE:
            write_kwargs: dict[str, Any] = {
                "column_nullability": self._data_model.column_nullability(),
                "table_sql": self._data_model.table_sql,
                "index_sql": self._data_model.index_sql.get(io_format, ()),
            }
        else:
            self._validate_embedding_batch(rows)
            write_kwargs = {"merge_keys": ["fund_id", "chat_id", "start_message_seq"]}
        with self.io:
            self._validate_parents(rows)
            self.io.write(rows.to_arrow(), self._db_path, **write_kwargs)

    def _check_io_supports_model(self) -> None:
        from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel

        if self._data_model.fund_id != self._fund_id:
            raise ValueError("The data model's fund scope cannot be changed")
        if (
            not isinstance(self._data_model, AlphaFundDataModel)
            and self._fund_id is None
        ):
            raise ValueError("AlphaFund entity storage requires fund_id")
        if self._is_batch_model() and self.io.IO_FORMAT != IOFormat.LANCEDB:
            raise TypeError(
                "AlphaFund embeddings require LanceDB IO, " + f"got {self.io.IO_FORMAT}"
            )
        if not self._is_batch_model() and self.io.IO_FORMAT != IOFormat.SQLITE:
            raise TypeError("AlphaFund entity storage requires SQLite IO")

    def _scope_filter(
        self, where: str | None, params: tuple[Any, ...] = ()
    ) -> tuple[str | None, tuple[Any, ...]]:
        if self._fund_id is None:
            return where, params
        if self.io.IO_FORMAT == IOFormat.LANCEDB:
            scope = f"fund_id = {self._quote_literal(str(self._fund_id))}"
        else:
            scope = '"fund_id" = ?'
            params = (*params, str(self._fund_id))
        return f"({where}) AND {scope}" if where else scope, params

    def _validate_parents(self, rows: pl.DataFrame) -> None:
        """Validate relationships against the canonical SQLite entity store.

        Embeddings and entities use the same data root; vectors live in LanceDB,
        and entity ownership lives in alphafund.db alongside it.
        """
        from pfeed.io.sqlite_io import SQLiteIO

        table = self._data_model.table_name
        if table == "funds":
            return
        if self._is_batch_model():
            sqlite_path = str(
                FilePath(self._data_path)
                / f"{self._data_model.data_source.name.lower()}{SQLiteIO.FILE_EXTENSION}"
            )
            # Do not create an empty entity database when the caller chose a
            # vector root that has no corresponding entity store.
            if not FilePath(sqlite_path).exists():
                raise LookupError(
                    "The embedding data root has no AlphaFund entity database"
                )
            with SQLiteIO() as io:
                conn = io.connect(sqlite_path)
                self._check_parent_rows(conn, rows, [("chat_id", "chats", "chat_id")])
        else:
            assert self._db_path is not None
            conn = cast(SQLiteIO, self.io).connect(self._db_path.db_uri)
            relations = {
                "agents": [("fund_id", "funds", "fund_id")],
                "channels": [("fund_id", "funds", "fund_id")],
                "chats": [
                    ("channel_id", "channels", "channel_id"),
                    ("parent_message_id", "messages", "message_id"),
                ],
                "messages": [
                    ("chat_id", "chats", "chat_id"),
                    ("start_message_id", "messages", "message_id"),
                    ("end_message_id", "messages", "message_id"),
                ],
            }
            self._check_parent_rows(conn, rows, relations[table])

    def _check_parent_rows(
        self,
        conn: SQLiteConnection,
        rows: pl.DataFrame,
        relations: list[tuple[str, str, str]],
    ) -> None:
        for column, table, identity in relations:
            values = rows[column].drop_nulls().unique().to_list()
            if not values:
                continue
            if not conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table,),
            ).fetchone():
                raise LookupError(f"Parent {table} not found in this fund")
            for value in values:
                sql = (
                    f'SELECT 1 FROM "{table}" WHERE "{identity}" = ? AND "fund_id" = ?'
                )
                if not conn.execute(sql, (value, str(self._fund_id))).fetchone():
                    raise LookupError(f"Parent {table} not found in this fund")

    def _is_batch_model(self) -> bool:
        """Entity models are one row per write; embedding windows are written in batches."""
        from pfeed.sources.alphafund.embedding_data_model import (
            AlphaFundEmbeddingDataModel,
        )

        return isinstance(self._data_model, AlphaFundEmbeddingDataModel)

    def _validate_embedding_batch(self, rows: pl.DataFrame) -> None:
        model = cast("AlphaFundEmbeddingDataModel", self._data_model)
        keys = ["fund_id", "chat_id", "start_message_seq"]
        if rows.select(keys).is_duplicated().any():
            raise ValueError(
                "Embedding batch contains duplicate windows; "
                + f"each ({', '.join(keys)}) must appear once"
            )
        if (
            rows["embedding_model"].null_count()
            or (rows["embedding_model"] != model.embedding_model).any()
        ):
            raise ValueError("Embedding rows must match the table's embedding_model")

    def _update_batch(self, frame: pl.DataFrame) -> None:
        """Patch one existing entity by its UUID.

        Only the fields the caller set are written, so an update that carries
        new content does not reset ``is_deleted`` or ``is_archived`` to their
        defaults. ``created_at`` is never written.
        """
        if self.io.IO_FORMAT != IOFormat.SQLITE:
            raise NotImplementedError("AlphaFund updates currently require SQLite IO")
        io = cast("DatabaseIO", self.io)

        identity_column = self._data_model.identity_column
        identity = getattr(self._data_model, identity_column)
        if identity is None:
            raise ValueError(
                f"{identity_column} must be provided for an update operation"
            )
        identity_name = self._quote_identifier(identity_column)
        if self.read(where=f"{identity_name} = ?", params=(str(identity),)) is None:
            raise LookupError(
                f"Cannot update missing {self._data_model.table_name} row with "
                + f"{identity_column}={identity}"
            )

        row = frame.row(0, named=True)
        if row[identity_column] != str(identity):
            raise ValueError("Update row identity must match its data model")
        provided = self._data_model.model_fields_set
        mutable_columns = tuple(
            column
            for column in self._data_model.column_names()
            if column not in {identity_column, "created_at", "fund_id"}
            and column in provided
        )
        if not mutable_columns:
            raise ValueError("An update operation must set at least one column")
        assignments = ", ".join(
            f"{self._quote_identifier(column)} = ?" for column in mutable_columns
        )
        params = tuple(row[column] for column in mutable_columns) + (
            row[identity_column],
        )

        assert self._db_path is not None
        table_name = self._quote_identifier(self._data_model.table_name)
        where, params = self._scope_filter(f"{identity_name} = ?", params)
        sql = f"UPDATE {table_name} SET {assignments} WHERE {where}"

        with io:
            io.add_missing_columns(
                self._db_path, frame.to_arrow().schema, self._data_model.column_nullability()
            )
            conn = cast("SQLiteConnection", io.connect(self._db_path.db_uri))
            with conn:
                self._validate_parents(frame)
                cursor = conn.execute(sql, params)
                if cursor.rowcount != 1:
                    raise LookupError(
                        f"Expected to update one {self._data_model.table_name} row; "
                        + f"updated {cursor.rowcount}"
                    )

    def read(
        self,
        where: str | None = None,
        params: tuple[Any, ...] = (),
        columns: list[str] | None = None,
    ) -> pl.LazyFrame | None:
        self._check_io_supports_model()
        if where is None:
            where, params = self._default_read_filter()
        where, params = self._scope_filter(where, params)
        assert self._db_path is not None
        read_kwargs: dict[str, Any] = {}
        if params:
            read_kwargs["params"] = params
        if columns and self.io.IO_FORMAT == IOFormat.LANCEDB:
            read_kwargs["columns"] = columns
        with self.io:
            result = cast(
                "pl.LazyFrame | None",
                self.io.read(self._db_path, where=where, **read_kwargs),
            )
        # A missing table returns None, while an existing table with no matching
        # rows returns an empty LazyFrame. Both mean no stored AlphaFund result.
        if result is None or result.limit(1).collect().is_empty():
            return None
        if columns and self.io.IO_FORMAT == IOFormat.SQLITE:
            result = result.select(columns)
        return result

    def search(
        self,
        query_vector: Sequence[float] | None = None,
        query_text: str | None = None,
        limit: int = 10,
        chat_ids: Sequence[UUID] | None = None,
        **search_kwargs: Any,
    ) -> pl.LazyFrame | None:
        """Search embedding rows; the data model's chat_id/embedding_model narrow the scope."""
        self._check_io_supports_model()
        if not self._is_batch_model():
            raise TypeError("search is only supported for AlphaFund embeddings")
        where, _ = self._default_read_filter()
        if chat_ids:
            literals = ", ".join(self._quote_literal(str(value)) for value in chat_ids)
            where = f"({where}) AND chat_id IN ({literals})"
        custom_where = search_kwargs.pop("where", None)
        if custom_where:
            where = f"({where}) AND ({custom_where})"
        where, _ = self._scope_filter(where)
        assert self._db_path is not None
        io = cast("DatabaseIO", self.io)
        with io:
            result = io.search(
                self._db_path,
                query_vector=query_vector,
                query_text=query_text,
                limit=limit,
                where=where or None,
                **search_kwargs,
            )
        if result is None or result.limit(1).collect().is_empty():
            return None
        return result

    def create_search_index(self, **index_kwargs: Any) -> None:
        self._check_io_supports_model()
        if not self._is_batch_model():
            raise TypeError(
                "search indexes are only supported for AlphaFund embeddings"
            )
        assert self._db_path is not None
        io = cast("DatabaseIO", self.io)
        with io:
            io.create_search_index(self._db_path, **index_kwargs)

    def _default_read_filter(self) -> tuple[str, tuple[Any, ...]]:
        from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
        from pfeed.sources.alphafund.channel_data_model import (
            AlphaFundChannelDataModel,
        )
        from pfeed.sources.alphafund.chat_data_model import AlphaFundChatDataModel
        from pfeed.sources.alphafund.embedding_data_model import (
            AlphaFundEmbeddingDataModel,
        )
        from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel
        from pfeed.sources.alphafund.message_data_model import (
            AlphaFundMessageDataModel,
        )

        model = self._data_model
        match model:
            case AlphaFundEmbeddingDataModel():
                # Embedding rows live in LanceDB, whose predicates are literal SQL
                # with no parameter binding. The table is already per model;
                # the fund always scopes, the chat optionally narrows.
                if model.fund_id is None:
                    raise ValueError("An embedding lookup requires fund_id")
                clauses = [f"fund_id = {self._quote_literal(str(model.fund_id))}"]
                if model.chat_id is not None:
                    clauses.append(
                        f"chat_id = {self._quote_literal(str(model.chat_id))}"
                    )
                return " AND ".join(clauses), ()
            case AlphaFundDataModel() if model.fund_id is not None:
                return '"fund_id" = ?', (str(model.fund_id),)
            case AlphaFundDataModel() if (
                model.user_id is not None and model.fund_name is not None
            ):
                return (
                    '"user_id" = ? AND "fund_name" = ?',
                    (str(model.user_id), model.fund_name),
                )
            case AlphaFundDataModel() if model.user_id is not None:
                return '"user_id" = ?', (str(model.user_id),)
            case AlphaFundDataModel():
                raise ValueError("A fund lookup requires user_id or fund_id")
            case AlphaFundAgentDataModel() if (
                model.agent_id is not None and model.agent_role is not None
            ):
                return (
                    '"agent_id" = ? AND LOWER("agent_role") = LOWER(?)',
                    (str(model.agent_id), model.agent_role),
                )
            case AlphaFundAgentDataModel() if model.agent_id is not None:
                return '"agent_id" = ?', (str(model.agent_id),)
            case AlphaFundAgentDataModel() if (
                model.fund_id is not None
                and model.agent_name is not None
                and model.agent_role is not None
            ):
                return (
                    '"fund_id" = ? AND "agent_name" = ? '
                    + 'AND LOWER("agent_role") = LOWER(?)',
                    (str(model.fund_id), model.agent_name, model.agent_role),
                )
            case AlphaFundAgentDataModel() if (
                model.fund_id is not None and model.agent_name is not None
            ):
                return (
                    '"fund_id" = ? AND "agent_name" = ?',
                    (str(model.fund_id), model.agent_name),
                )
            case AlphaFundAgentDataModel() if (
                model.fund_id is not None and model.agent_role is not None
            ):
                return (
                    '"fund_id" = ? AND LOWER("agent_role") = LOWER(?)',
                    (str(model.fund_id), model.agent_role),
                )
            case AlphaFundAgentDataModel() if model.fund_id is not None:
                return '"fund_id" = ?', (str(model.fund_id),)
            case AlphaFundAgentDataModel() if (
                model.agent_role is not None and model.agent_name is None
            ):
                return 'LOWER("agent_role") = LOWER(?)', (model.agent_role,)
            case AlphaFundAgentDataModel():
                raise ValueError(
                    "An agent lookup requires fund_id, agent_role, or agent_id; "
                    + "agent_name also requires fund_id"
                )
            case AlphaFundChannelDataModel() if model.channel_id is not None:
                return '"channel_id" = ?', (str(model.channel_id),)
            case AlphaFundChannelDataModel() if (
                model.fund_id is not None and model.channel_name is not None
            ):
                return (
                    '"fund_id" = ? AND "channel_name" = ?',
                    (str(model.fund_id), model.channel_name),
                )
            case AlphaFundChannelDataModel() if model.fund_id is not None:
                return '"fund_id" = ?', (str(model.fund_id),)
            case AlphaFundChannelDataModel():
                raise ValueError("A channel lookup requires fund_id or channel_id")
            case AlphaFundChatDataModel():
                if model.chat_id is not None:
                    return '"chat_id" = ?', (str(model.chat_id),)
                if model.channel_id is not None and model.chat_name is not None:
                    return (
                        '"channel_id" = ? AND "chat_name" = ?',
                        (str(model.channel_id), model.chat_name),
                    )
                if model.channel_id is not None:
                    return '"channel_id" = ?', (str(model.channel_id),)
                raise ValueError("A chat lookup requires channel_id or chat_id")
            case AlphaFundMessageDataModel() if model.message_id is not None:
                return '"message_id" = ?', (str(model.message_id),)
            case AlphaFundMessageDataModel() if model.chat_id is not None:
                return '"chat_id" = ?', (str(model.chat_id),)
            case AlphaFundMessageDataModel():
                raise ValueError("A message lookup requires chat_id or message_id")
            case _:
                assert_never(model)

    def _validate_schema(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Pin every column to the data model's declared storage dtype."""
        return data.select(
            pl.col(column_name).cast(dtype)
            for column_name, dtype in self._data_model.polars_schema().items()
        )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """Quote a trusted model-declared SQLite identifier."""
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _quote_literal(value: str) -> str:
        """Quote an internal (non-user-controlled) value as a SQL string literal."""
        return "'" + value.replace("'", "''") + "'"

    def _create_file_path(self, *args: Any, **kwargs: Any) -> FilePath:
        raise NotImplementedError("AlphaFund data requires database IO")

    def _create_table_path(self, *args: Any, **kwargs: Any) -> TablePath:
        raise NotImplementedError("AlphaFund data requires database IO")

    def _create_db_path(self, *args: Any, **kwargs: Any) -> DBPath:
        data_model = self._data_model
        db_name = data_model.data_source.name.lower()

        if self.io.is_file_io(strict=False):
            # Single-file backends (SQLite, DuckDB) carry an extension; directory
            # backends (LanceDB) do not.
            extension = self.io.FILE_EXTENSION or ""
            db_uri = str(cast(FilePath, self._data_path) / f"{db_name}{extension}")
        else:
            db_uri = f"{str(self._data_path).rstrip('/')}/{db_name}"

        return DBPath(
            db_uri=db_uri,
            db_name=db_name,
            table_name=data_model.table_name,
        )

    def _create_metadata(self, *args: Any, **kwargs: Any) -> BaseDataMetadata:
        return BaseDataMetadata(
            data_source=DataSource[self._data_model.data_source.name],
            data_origin=self._data_model.data_origin,
        )
