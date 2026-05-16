from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, cast
if TYPE_CHECKING:
    from pfeed.streaming.streaming_message import StreamingMessage
    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.data_handlers.base_data_handler import SourcePath
    from pfeed.storages.database_storage import DatabaseURI

import datetime

import polars as pl
from pydantic import BaseModel, Field

from pfund.datas.resolution import Resolution
from pfeed.utils.file_path import FilePath
from pfeed._io.table_io import TablePath
from pfeed._io.database_io import DBPath
from pfeed.enums import StreamMode, DataSource
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler, TimeBasedDataMetadata
from pfeed.data_handlers.streaming_data_handler_mixin import StreamingDataHandlerMixin


ProductSymbol: TypeAlias = str


class ProductMetadata(BaseModel):
    basis: str
    symbol: str
    specs: dict[str, Any] = Field(
        default_factory=dict,
        description='specifications that make a product unique, e.g. for options, specs are strike_price, expiration_date, etc.'
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
        from pandera.config import config_context, ValidationDepth
        from pfeed.schemas import MarketDataSchema, TickDataSchema, BarDataSchema
        data_model: MarketDataModel = self._data_model
        resolution: Resolution = data_model.resolution
        if resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
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
        filename = '_'.join([product.symbol, str(date)])
        file_extension = self._get_file_extension()
        year, month, day = str(date).split('-')
        table_path = self._create_table_path()
        return FilePath(
            table_path
            / f'year={year}'
            / f'month={month}'
            / f'day={day}'
            / (filename + file_extension)
        )

    def _create_table_path(self) -> TablePath:
        data_model: MarketDataModel = self._data_model
        product = data_model.product
        return TablePath(
            cast(FilePath, self._data_path)
            / f"env={data_model.env}"
            / f"data_layer={self._data_layer}"
            / f'data_domain={self._data_domain}'
            / f'data_source={data_model.data_source.name}'
            / f'data_origin={data_model.data_origin}'
            / f'asset_type={str(product.asset_type)}'
            / f'resolution={str(data_model.resolution)}'
        )

    def _create_db_path(self):
        data_model = self._data_model
        product = data_model.product
        db_name = data_model.env.lower()
        schema_name = "_".join([
            f"{self._data_layer}",
            f"{self._data_domain}",
            f"{data_model.data_source.name}",
            f"{data_model.data_origin}",
        ]).lower()
        table_name = '_'.join([
            str(product.asset_type),
            str(data_model.resolution),
        ]).lower()
        # NOTE: special case "lancedb" where its table io and database io at the same time
        if self._is_table_io(strict=False):
            table_path = self._create_table_path()
            table_path = table_path.parents[1]  # remove levels "asset_type" and "resolution"
            db_uri = str(table_path)
        # NOTE: special case "duckdb" where its file io and database io at the same time
        elif self._is_file_io(strict=False):
            file_extension = self._io.FILE_EXTENSION
            assert file_extension is not None
            db_uri = cast(FilePath, self._data_path) / (db_name + file_extension)
            db_uri = str(db_uri)
        else:
            db_uri = cast(DatabaseURI, self._data_path) + '/' + db_name
        return DBPath(db_uri=db_uri, db_name=db_name, schema_name=schema_name, table_name=table_name)

    def _create_metadata(self, dates: list[datetime.date]) -> MarketDataMetadata:
        symbol: ProductSymbol = self._data_model.product.symbol
        products = {
            symbol: ProductMetadata(
                basis=str(self._data_model.product.basis),
                symbol=symbol,
                specs=self._data_model.product.specs,
            )
        }
        if self._is_table_io() or self._is_database_io():
            source_path = self._table_path if self._is_table_io() else self._db_path
            assert source_path is not None, f'source_path is not set for {self._io.name}'
            existing_metadata_dict = cast(dict[SourcePath, MarketDataMetadata], self.read_metadata())
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
            resolution=repr(self._data_model.resolution),
            asset_type=str(self._data_model.product.asset_type),
        )

    def _create_stream_writer(self, stream_mode: StreamMode, flush_interval: int):
        from pfeed.streaming.stream_buffer import StreamBuffer
        if self._is_streaming_io():
            # EXTEND: only support deltalake_io for now
            if self._is_table_io():
                buffer_path = self._table_path
                assert buffer_path is not None, 'buffer_path is required for table io'
                self._stream_writer = StreamBuffer(
                    io=self._io,
                    buffer_path=buffer_path,
                    stream_mode=stream_mode,
                    flush_interval=flush_interval
                )
            # TODO: writing streaming data to database is not supported yet
            else:
                raise ValueError(f"Streaming is not supported for {self._io.name}")
        else:
            raise ValueError(f"Streaming is not supported for {self._io.name}")

    # NOTE: streaming data (env=LIVE/PAPER) does NOT follow the same columns in write_batch (env=BACKTEST)
    def _standardize_streaming_msg(self, msg: StreamingMessage) -> dict:
        """
        Convert StreamingMessage to dict and standardize it, e.g. convert timestamp to datetime (UTC)
        """
        data = msg.to_dict()

        # drop empty dicts
        dict_fields = ["extra_data"]
        for field in dict_fields:
            if not data[field]:
                data.pop(field)

        # flatten specs, make each spec a column, e.g. strike_price is now a column
        for k, v in data["specs"].items():
            data[k] = v
        data.pop("specs")

        # convert timestamp to datetime (UTC)
        date = datetime.datetime.fromtimestamp(
            data["ts"], tz=datetime.timezone.utc
        ).replace(tzinfo=None)
        data["date"] = date

        # add year, month, day columns for delta table partitioning
        if self._supports_partitioning():
            data["year"] = date.year
            data["month"] = date.month
            data["day"] = date.day

        return data

    # EXTEND: currently only supports writing for parquet+deltalake (using .arrow for buffering)
    def _write_stream(self, data: dict[str, Any] | StreamingMessage):
        # TODO: check to ensure data is not raw
        print(f'***WRITE STREAM GOT*** {data}')
        # data =I self._standardize_streaming_msg(data)
        # self._stream_writer.write(
        #     data,
        #     metadata=self._data_model.to_metadata(),
        #     partition_by=self.PARTITION_COLUMNS,
        # )
