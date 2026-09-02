from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, assert_never, cast

if TYPE_CHECKING:
    from sqlite3 import Connection as SQLiteConnection

    from narwhals.typing import IntoFrame

    from pfeed.io.database_io import DatabaseIO
    from pfeed.sinks.base_sink import BaseSink
    from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
    from pfeed.sources.alphafund.channel_data_model import (
        AlphaFundChannelDataModel,
    )
    from pfeed.sources.alphafund.chat_data_model import AlphaFundChatDataModel
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
        super().__init__(
            data_path=data_path,
            data_layer=data_layer,
            data_domain=data_domain,
            data_model=data_model,
            io=io,
            sink=sink,
        )

    def write_batch(self, data: IntoFrame, *args: Any, **kwargs: Any) -> None:
        frame = cast(pl.LazyFrame, convert_dataframe(data, DataTool.polars))
        frame = self._validate_schema(frame)
        rows = frame.collect()
        if rows.height != 1:
            raise ValueError(
                "An AlphaFund create or update operation must contain exactly one row"
            )

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
        index_sql = self._data_model.index_sql.get(io_format, ()) if io_format else ()
        with self.io:
            self.io.write(
                rows.to_arrow(),
                self._db_path,
                column_nullability=self._data_model.column_nullability(),
                table_sql=self._data_model.table_sql,
                index_sql=index_sql,
            )

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
        provided = self._data_model.model_fields_set
        mutable_columns = tuple(
            column
            for column in self._data_model.column_names()
            if column not in {identity_column, "created_at"} and column in provided
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
        sql = f"UPDATE {table_name} SET {assignments} WHERE {identity_name} = ?"

        with io:
            conn = cast("SQLiteConnection", io.connect(self._db_path.db_uri))
            with conn:
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
    ) -> pl.LazyFrame | None:
        if where is None:
            where, params = self._default_read_filter()
        assert self._db_path is not None
        with self.io:
            result = cast(
                "pl.LazyFrame | None",
                self.io.read(self._db_path, where=where, params=params),
            )
        # A missing table returns None, while an existing table with no matching
        # rows returns an empty LazyFrame. Both mean no stored AlphaFund result.
        if result is None or result.limit(1).collect().is_empty():
            return None
        return result

    def _default_read_filter(self) -> tuple[str, tuple[Any, ...]]:
        from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
        from pfeed.sources.alphafund.channel_data_model import (
            AlphaFundChannelDataModel,
        )
        from pfeed.sources.alphafund.chat_data_model import AlphaFundChatDataModel
        from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel
        from pfeed.sources.alphafund.message_data_model import (
            AlphaFundMessageDataModel,
        )

        model = self._data_model
        match model:
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

    def _create_file_path(self, *args: Any, **kwargs: Any) -> FilePath:
        raise NotImplementedError("AlphaFund data requires database IO")

    def _create_table_path(self, *args: Any, **kwargs: Any) -> TablePath:
        raise NotImplementedError("AlphaFund data requires database IO")

    def _create_db_path(self, *args: Any, **kwargs: Any) -> DBPath:
        data_model = self._data_model
        db_name = data_model.data_source.name.lower()

        if self.io.is_file_io(strict=False):
            extension = self.io.FILE_EXTENSION
            assert extension is not None
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
