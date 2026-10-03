from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, cast

if TYPE_CHECKING:
    from pfund.datas.resolution import Resolution

    from pfeed.io.base_io import Metadata, Partition, PartitionValue

import polars as pl

from pfeed.data_handlers.base_data_handler import BaseDataMetadata
from pfeed.data_handlers.time_based_data_handler import TimeBasedDataHandler
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.enums import DataLayer


class MarketDataMetadata(BaseDataMetadata):
    pass


class MarketDataHandler(TimeBasedDataHandler[MarketDataModel, MarketDataMetadata]):
    """Stores one product's market data over a date range, one partition per (product, day).

    Partition levels:
    - product: cleaned data already has it; raw data doesn't, so it is added on write and dropped on read
    - DatePartition of the date column: the IO computes each row's date from it, nothing is added
    """

    PRODUCT_PARTITION_COL: ClassVar[str] = 'product'

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

    def _parse_metadata(self, metadata: Metadata) -> MarketDataMetadata:
        return MarketDataMetadata.model_validate(metadata)

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
        metadata = MarketDataMetadata()
        self._write(df, {partition: metadata for partition in self._partitions()}, mode='replace')

    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, MarketDataMetadata]]:
        lf, metadata = self._read(self._partitions())
        if lf is not None and self._data_layer == DataLayer.RAW:
            lf = lf.drop(self.PRODUCT_PARTITION_COL)
        return lf, metadata
