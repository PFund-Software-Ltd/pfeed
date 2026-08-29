from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, assert_never, cast

if TYPE_CHECKING:
    from narwhals.typing import IntoFrame

    from pfeed.io.database_io import DatabaseIO
    from pfeed.sinks.base_sink import BaseSink
    from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
    from pfeed.sources.alphafund.channel_data_model import (
        AlphaFundChannelDataModel,
    )
    from pfeed.sources.alphafund.chat_data_model import AlphaFundChatDataModel
    from pfeed.sources.alphafund.message_data_model import (
        AlphaFundMessageDataModel,
    )
    from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel
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
from pfeed.enums import DataLayer, DataSource, DataTool
from pfeed.io.database_io import DBPath
from pfeed.io.table_io import TablePath
from pfeed.utils.file_path import FilePath


class AlphaFundDataHandler(BaseDataHandler):
    """Persists every AlphaFund SQL model in its table in one SQLite database."""

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
        assert self._db_path is not None
        io_format = self.io.IO_FORMAT
        index_sql = self._data_model.index_sql.get(io_format, ()) if io_format else ()
        insert_sql = self._data_model.insert_sql.get(io_format, "") if io_format else ""
        with self.io:
            self.io.write(
                frame.collect().to_arrow(),
                self._db_path,
                column_nullability=self._data_model.column_nullability(),
                table_sql=self._data_model.table_sql,
                index_sql=index_sql,
                insert_sql=insert_sql,
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
            return self.io.read(self._db_path, where=where, params=params)

    def _default_read_filter(self) -> tuple[str, tuple[Any, ...]]:
        from pfeed.sources.alphafund.agent_data_model import AlphaFundAgentDataModel
        from pfeed.sources.alphafund.channel_data_model import (
            AlphaFundChannelDataModel,
        )
        from pfeed.sources.alphafund.chat_data_model import AlphaFundChatDataModel
        from pfeed.sources.alphafund.message_data_model import (
            AlphaFundMessageDataModel,
        )
        from pfeed.sources.alphafund.fund_data_model import AlphaFundDataModel

        model = self._data_model
        match model:
            case AlphaFundDataModel():
                return (
                    '"user_id" = ? AND "fund_name" = ?',
                    (str(model.user_id), model.fund_name),
                )
            case AlphaFundAgentDataModel():
                return (
                    '"fund_id" = ? AND "agent_name" = ?',
                    (str(model.fund_id), model.agent_name),
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
            case AlphaFundChannelDataModel():
                return '"fund_id" = ?', (str(model.fund_id),)
            case AlphaFundChatDataModel():
                return (
                    '"channel_id" = ? AND "chat_id" = ?',
                    (str(model.channel_id), str(model.chat_id)),
                )
            case AlphaFundMessageDataModel():
                return '"chat_id" = ?', (str(model.chat_id),)
            case _:
                assert_never(model)

    def _validate_schema(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Pin every column to the data model's declared storage dtype."""
        return data.select(
            pl.col(column_name).cast(dtype)
            for column_name, dtype in self._data_model.polars_schema().items()
        )

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
