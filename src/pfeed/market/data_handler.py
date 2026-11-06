from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    import pyarrow as pa
    from pfund_blocks.datas.tick_data import Tick
    from pfund_blocks.resolution import Resolution
    from pfund_blocks.typing import Message

    from pfeed.io.base_io import BaseIO, Partition, PartitionValue
    from pfeed.market.data_model import MarketDataModel

import polars as pl

from pfeed.base.data_handler import BaseDataMetadata
from pfeed.base.time_based_data_handler import TimeBasedDataHandler
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

    def __init__(
        self,
        data_model: MarketDataModel,
        io: BaseIO,
        data_layer: DataLayer | str = DataLayer.CLEANED,
        data_domain: str = DataCategory.MARKET_DATA,
        is_streaming: bool = False,
        store_incremental_bars: bool = False,
    ):
        super().__init__(data_model, io, data_domain=data_domain, data_layer=data_layer, is_streaming=is_streaming)
        self._store_incremental_bars = store_incremental_bars

    def _create_namespace(self) -> dict[str, str]:
        data_model = self._data_model
        return {
            'env': str(data_model.env),
            'data_layer': str(self._data_layer),
            'data_domain': self._data_domain,
            'data_source': data_model.data_source,
            'data_origin': str(data_model.data_origin),
        }

    def _create_name(self) -> dict[str, str]:
        return {
            'product_type': str(self._data_model.product.product_type),
            'resolution': str(self._data_model.resolution),
        }

    def _partition_prefix(self) -> dict[str, PartitionValue]:
        from pfeed.schemas.market_data_schema import PRODUCT

        return {PRODUCT: self._data_model.product.name}

    def _get_date_col(self) -> str:
        if self._is_streaming:
            from pfeed.schemas.streaming.bar_message_schema import START_TS
            from pfeed.schemas.streaming.tick_message_schema import TS

            return START_TS if cast('Resolution', self._data_model.resolution).is_bar() else TS
        return super()._get_date_col()

    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        from pfeed.schemas.market_data_schema import get_market_data_schema

        resolution = cast('Resolution', self._data_model.resolution)
        from pfeed.schemas.market_data_schema import RESOLUTION

        schema = get_market_data_schema(self._data_model.product, resolution)
        df = schema.validate(df)
        # the schema only checks the resolution is valid and unique, it must also be the dataset's (in its name)
        # NOTE: product doesn't need this, a row of another product is outside the partitions and the IO raises
        if not df.is_empty() and (row_resolution := df[RESOLUTION][0]) != str(resolution):
            raise ValueError(f'data has resolution {row_resolution!r}, expected {str(resolution)!r}')
        return df

    def write_batch(self, df: pl.DataFrame) -> None:
        """Replaces the data model's partitions with `df`.

        Every day in the data model's date range gets a partition, so a day without rows
        (e.g. a holiday) is stored as an empty partition and isn't re-downloaded.
        `df` must not have rows outside the date range, otherwise the IO raises.
        """
        from pfeed.schemas.market_data_schema import PRODUCT

        if self._data_layer == DataLayer.RAW:
            if PRODUCT in df.columns:
                raise ValueError(f'raw data already has column {PRODUCT!r}, it is reserved for partitioning')
            df = df.with_columns(pl.lit(self._data_model.product.name).alias(PRODUCT))
        else:
            df = self._validate_schema(df)
        self._write(df, {partition: self._create_metadata() for partition in self._partitions()}, mode='replace')

    def _build_streaming_schema(self) -> pa.Schema:
        from pfeed.schemas.streaming.bar_message_schema import BarMessageSchema
        from pfeed.schemas.streaming.tick_message_schema import TickMessageSchema

        resolution = cast('Resolution', self._data_model.resolution)
        if resolution.is_tick():
            message_schema = TickMessageSchema
        elif resolution.is_bar():
            message_schema = BarMessageSchema
        else:
            raise NotImplementedError(f'streaming {resolution} data is not supported yet')
        return message_schema.build()

    def write_stream(self, msg: Message) -> None:
        from pfund_blocks.datas.bar_data import Bar
        if self._stream_buffer is None:
            raise ValueError(f'{self!r} is not set up for streaming, call setup_streaming() first')
        data_points = cast("list[Bar | Tick] | None", msg['data'])
        # e.g. a resampled message whose update didn't change the resampled bar
        if data_points is None:
            return
        # NOTE: a parsed message can hold several data points (e.g. Bybit sends a list), so can a resampled one, e.g. several closed bars
        for data_point in data_points:
            if isinstance(data_point, Bar) and not data_point.is_closed and not self._store_incremental_bars:
                continue
            # columns not in the streaming schema (e.g. resolution) are dropped when the buffer builds its dataframe
            row = {'product': msg['product'], 'received_at': msg['received_at'], **data_point.to_dict()}
            self._stream_buffer.append(row)
        self.flush_if_due()

    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, MarketDataMetadata]]:
        from pfeed.schemas.market_data_schema import PRODUCT

        lf, metadata = self._read(self._partitions())
        if lf is not None and self._data_layer == DataLayer.RAW:
            lf = lf.drop(PRODUCT)
        return lf, metadata
