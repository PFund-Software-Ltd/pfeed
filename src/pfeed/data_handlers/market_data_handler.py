from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    import pyarrow as pa
    from pfund.datas.resolution import Resolution

    from pfeed.data_models.market_data_model import MarketDataModel
    from pfeed.io.base_io import BaseIO, Partition, PartitionValue
    from pfeed.streaming.market_data_message import MarketDataMessage
    from pfeed.streaming.sink import Sink

import polars as pl
from pfund.enums.env import Environment

from pfeed.data_handlers.base_data_handler import BaseDataMetadata
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler
from pfeed.enums import DataCategory, DataLayer


class MarketDataMetadata(BaseDataMetadata):
    pass


class MarketDataHandler(TimeBasedDataHandler["MarketDataModel", MarketDataMetadata]):
    """Stores one product's market data over a date range, one partition per (product, day).

    Partition levels:
    - product: cleaned data already has it; raw data doesn't, so it is added on write and dropped on read
    - DatePartition of the date column: the IO computes each row's date from it, nothing is added

    Batch data (env=BACKTEST) and streamed data (other envs) have different columns, see _get_date_col().
    """

    Metadata = MarketDataMetadata
    PRODUCT_PARTITION_COL: ClassVar[str] = 'product'

    def __init__(
        self,
        data_model: MarketDataModel,
        io: BaseIO,
        data_layer: DataLayer | str = DataLayer.CLEANED,
        data_domain: str = DataCategory.MARKET_DATA,
        sink: Sink | None = None,
    ):
        super().__init__(data_model, io, data_domain=data_domain, data_layer=data_layer, sink=sink)
        if sink is not None and not self._is_streamed():
            raise ValueError(f'env={data_model.env} is not streamed, it cannot have a sink')
        if sink is not None and self._data_layer == DataLayer.RAW:
            raise NotImplementedError('storing streamed raw data is not supported yet')

    def _is_streamed(self) -> bool:
        """Data of env=BACKTEST is downloaded in batches, data of other envs is streamed."""
        return self._data_model.env != Environment.BACKTEST

    def _get_date_col(self) -> str:
        """Streamed data has no 'date' column: ticks use 'ts', bars 'start_ts' so a bar is in the day it starts."""
        if self._is_streamed() and self._data_layer != DataLayer.RAW:
            return 'start_ts' if cast('Resolution', self._data_model.resolution).is_bar() else 'ts'
        return super()._get_date_col()

    def _create_namespace(self) -> dict[str, str]:
        data_model = self._data_model
        return {
            'env': str(data_model.env),
            'data_layer': str(self._data_layer),
            'data_domain': self._data_domain,
            'data_source': str(data_model.data_source.name),
            'data_origin': str(data_model.data_origin),
        }

    def _create_name(self) -> dict[str, str]:
        return {
            'asset_type': str(self._data_model.product.asset_type),
            'resolution': str(self._data_model.resolution),
        }

    def _partition_prefix(self) -> dict[str, PartitionValue]:
        return {self.PRODUCT_PARTITION_COL: self._data_model.product.name}

    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        from pfeed.schemas import BarDataSchema, MarketDataSchema, TickDataSchema

        resolution = cast('Resolution', self._data_model.resolution)
        if resolution.is_quote():
            raise NotImplementedError('quote data is not supported yet')
        elif resolution.is_tick():
            schema = TickDataSchema
        elif resolution.is_bar():
            schema = BarDataSchema
        else:
            schema = MarketDataSchema
        return schema.validate(df)

    def write_batch(self, df: pl.DataFrame) -> None:
        """Replaces the data model's partitions with `df`.

        Every day in the data model's date range gets a partition, so a day without rows
        (e.g. a holiday) is stored as an empty partition and isn't re-downloaded.
        `df` must not have rows outside the date range, otherwise the IO raises.
        """
        if self._data_layer == DataLayer.RAW:
            if self.PRODUCT_PARTITION_COL in df.columns:
                raise ValueError(f'raw data already has column {self.PRODUCT_PARTITION_COL!r}, it is reserved for partitioning')
            df = df.with_columns(pl.lit(self._data_model.product.name).alias(self.PRODUCT_PARTITION_COL))
        else:
            df = self._validate_schema(df)
        self._write(df, {partition: self._create_metadata() for partition in self._partitions()}, mode='replace')

    def _build_streaming_schema(self) -> pa.Schema:
        from pfeed.schemas.bar_message_schema import BarMessageSchema
        from pfeed.schemas.specs_schemas import get_specs_schema
        from pfeed.schemas.tick_message_schema import TickMessageSchema

        resolution = cast('Resolution', self._data_model.resolution)
        if resolution.is_tick():
            message_schema = TickMessageSchema
        elif resolution.is_bar():
            message_schema = BarMessageSchema
        else:
            raise NotImplementedError(f'streaming {resolution} data is not supported yet')
        return message_schema.build(specs_schema=get_specs_schema(self._data_model.product))

    def write_stream(self, msg: MarketDataMessage, *, store_incremental_bars: bool = False, **kwargs: Any) -> None:
        """Buffers `msg` in the sink, and flushes the sink if a flush is due.

        Args:
            store_incremental_bars: if True, also stores the updates of a bar before it closes,
                e.g. for a venue that only streams such updates; otherwise only closed bars are stored.
            kwargs: only to match BaseDataHandler.write_stream(), any option raises TypeError.
        """
        if kwargs:
            raise TypeError(f'unexpected options {list(kwargs)}')
        if self._sink is None:
            raise ValueError(f'{self!r} has no sink, it cannot write streamed data')
        from pfeed.streaming.bar_message import BarMessage

        if isinstance(msg, BarMessage) and msg.is_incremental and not store_incremental_bars:
            return
        row = msg.to_dict()
        # REVIEW: extra is dropped, only the columns in the streaming schema are stored
        del row['extra']
        # one column per spec, e.g. strike_price for options
        row |= row.pop('specs')
        self._sink.append(row)
        if self._sink.is_due():
            self.flush()

    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, MarketDataMetadata]]:
        lf, metadata = self._read(self._partitions())
        if lf is not None and self._data_layer == DataLayer.RAW:
            lf = lf.drop(self.PRODUCT_PARTITION_COL)
        return lf, metadata
