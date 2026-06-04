# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, assert_never, cast

if TYPE_CHECKING:
    import pyarrow as pa

    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.storages.database_storage import DatabaseURI
    from pfeed.streaming.market_data_message import MarketDataMessage

import datetime

import polars as pl
from pfund.datas.resolution import Resolution
from pydantic import BaseModel, Field

from pfeed._io.database_io import DBPath
from pfeed._io.table_io import TablePath
from pfeed.data_handlers.base_data_handler import SourcePath
from pfeed.data_handlers.streaming_data_handler_mixin import StreamingDataHandlerMixin
from pfeed.data_handlers.time_based_data_handler import (
    TimeBasedDataHandler,
    TimeBasedDataMetadata,
)
from pfeed.enums import DataSource, IOType
from pfeed.utils.file_path import FilePath

ProductSymbol: TypeAlias = str


class ProductMetadata(BaseModel):
    basis: str
    symbol: str
    specs: dict[str, Any] = Field(
        default_factory=dict,
        description="specifications that make a product unique, e.g. for options, specs are strike_price, expiration_date, etc.",
    )


class MarketDataMetadata(TimeBasedDataMetadata):
    # NOTE: data can have multiple products in e.g. delta table (table io)
    products: dict[ProductSymbol, ProductMetadata] = Field(default_factory=dict)
    resolution: str
    asset_type: str


class MarketDataHandler(StreamingDataHandlerMixin, TimeBasedDataHandler):  # pyright: ignore[reportImplicitAbstractClass]
    _data_model: MarketDataModel
    metadata_class: ClassVar[type[MarketDataMetadata]] = MarketDataMetadata

    def _validate_schema(self, df: pl.LazyFrame) -> pl.LazyFrame:
        from pandera.config import ValidationDepth, config_context

        from pfeed.schemas import BarDataSchema, MarketDataSchema, TickDataSchema

        data_model: MarketDataModel = self._data_model
        resolution: Resolution = data_model.resolution
        if resolution.is_quote():
            raise NotImplementedError("quote data is not supported yet")
        elif resolution.is_tick():
            schema = TickDataSchema
        elif resolution.is_bar():
            schema = BarDataSchema
        else:
            schema = MarketDataSchema
        with config_context(validation_depth=ValidationDepth.SCHEMA_AND_DATA):
            return schema.validate(df)

    def _create_file_path(self, date: datetime.date) -> FilePath:
        data_model: MarketDataModel = self._data_model
        product = data_model.product
        filename = "_".join([product.symbol, str(date)])
        file_extension = self._get_file_extension()
        year, month, day = str(date).split("-")
        table_path = self._create_table_path()
        return FilePath(
            table_path
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
            / (filename + file_extension)
        )

    def _create_table_path(self) -> TablePath:
        data_model: MarketDataModel = self._data_model
        product = data_model.product
        return TablePath(
            cast(FilePath, self._data_path)
            / f"env={data_model.env}"
            / f"data_layer={self._data_layer}"
            / f"data_domain={self._data_domain}"
            / f"data_source={data_model.data_source.name}"
            / f"data_origin={data_model.data_origin}"
            / f"asset_type={product.asset_type!s}"
            / f"resolution={data_model.resolution!s}"
        )

    def _create_db_path(self) -> DBPath:
        data_model = self._data_model
        product = data_model.product
        db_name = data_model.env.lower()
        schema_name = "_".join(
            [
                f"{self._data_layer}",
                f"{self._data_domain}",
                f"{data_model.data_source.name}",
                f"{data_model.data_origin}",
            ]
        ).lower()
        table_name = "_".join(
            [
                str(product.asset_type),
                str(data_model.resolution),
            ]
        ).lower()

        match self._io_type:
            # NOTE: special case "lancedb" where its table io and database io at the same time
            case IOType.TABLE:
                table_path = self._create_table_path()
                table_path = table_path.parents[
                    1
                ]  # remove levels "asset_type" and "resolution"
                db_uri = str(table_path)
            # NOTE: special case "duckdb" where its file io and database io at the same time
            case IOType.FILE:
                file_extension = self.io.FILE_EXTENSION
                assert file_extension is not None
                db_uri = cast(FilePath, self._data_path) / (db_name + file_extension)
                db_uri = str(db_uri)
            case IOType.DATABASE:
                db_uri = cast("DatabaseURI", self._data_path) + "/" + db_name
            case _:
                assert_never(self._io_type)
        return DBPath(
            db_uri=db_uri,
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
        )

    def _create_sink_path(self) -> SourcePath:
        match self._io_type:
            case IOType.FILE:
                raise NotImplementedError(f"Unhandled {self.io} as a sink")
            case IOType.TABLE:
                sink_path = self._table_path
            case IOType.DATABASE:
                sink_path = self._db_path
            case _:
                assert_never(self._io_type)
        return sink_path  # pyright: ignore[reportReturnType]

    def _create_metadata(self, dates: list[datetime.date]) -> MarketDataMetadata:
        symbol: ProductSymbol = self._data_model.product.symbol
        products = {
            symbol: ProductMetadata(
                basis=str(self._data_model.product.basis),
                symbol=symbol,
                specs=self._data_model.product.specs,
            )
        }
        if self._io_type == IOType.TABLE or self._io_type == IOType.DATABASE:
            source_path = (
                self._table_path if self._io_type == IOType.TABLE else self._db_path
            )
            assert source_path is not None, f"source_path is not set for {self.io.name}"
            existing_metadata_dict = cast(
                dict[SourcePath, MarketDataMetadata], self.read_metadata()
            )
            existing_metadata = existing_metadata_dict.get(source_path, None)
            if existing_metadata is not None:
                # merge the current data model's metadata "dates" with the existing table metadata "dates"
                dates = list(set(existing_metadata.dates + dates))
                products = products | existing_metadata.products
        return MarketDataMetadata(
            data_source=DataSource[self._data_model.data_source.name],
            data_origin=self._data_model.data_origin,
            dates=dates,
            products=products,
            resolution=str(self._data_model.resolution),
            asset_type=str(self._data_model.product.asset_type),
        )

    def _build_streaming_schema(self) -> pa.Schema:
        from pfeed.schemas.bar_message_schema import BarMessageSchema
        from pfeed.schemas.specs_schemas import get_specs_schema
        from pfeed.schemas.tick_message_schema import TickMessageSchema

        data_model: MarketDataModel = self._data_model
        resolution: Resolution = data_model.resolution
        if resolution.is_quote():
            raise NotImplementedError("streaming quote data is not supported yet")
        elif resolution.is_tick():
            message_schema = TickMessageSchema
        elif resolution.is_bar():
            message_schema = BarMessageSchema
        else:
            raise ValueError(f"unsupported resolution for streaming: {resolution}")
        specs_schema = get_specs_schema(data_model.product)
        return message_schema.build(specs_schema=specs_schema)

    # NOTE: streaming data (env=LIVE/PAPER) does NOT follow the same column schema in write_batch (env=BACKTEST)
    def _standardize_streaming_msg(self, msg: MarketDataMessage) -> dict[str, Any]:
        """
        Convert MarketDataMessage to dict and standardize it: drop extra_data and flatten specs into top-level columns.
        """
        data = msg.to_dict()

        # REVIEW: drop extra_data, only support writing data defined in streaming schema
        dict_fields = ["extra_data"]
        for field in dict_fields:
            data.pop(field)

        # flatten specs, make each spec a column, e.g. strike_price is now a column
        for k, v in data["specs"].items():
            data[k] = v
        data.pop("specs")

        return data

    def write_stream(self, msg: MarketDataMessage):
        if msg.is_bar():
            sink_config = self.sink.config
            if msg.is_incremental and not sink_config.store_incremental_bars:  # pyright: ignore[reportAttributeAccessIssue]
                return
        data = self._standardize_streaming_msg(msg)
        self.sink.write(data, path=self._sink_path)  # pyright: ignore[reportArgumentType]
