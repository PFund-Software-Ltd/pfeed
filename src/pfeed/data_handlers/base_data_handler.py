from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pfeed.data_models.base_data_model import BaseDataModel
    from pfeed.io.base_io import BaseIO, DatasetKey, Metadata, Partition

from abc import ABC, abstractmethod

import polars as pl
from pydantic import BaseModel, ConfigDict

from pfeed.enums import DataLayer


class BaseDataMetadata(BaseModel):
    """Per-partition metadata, stored by the IO as JSON (see BaseIO commit marker).

    Only holds what the DatasetKey and the partition values can't express,
    e.g. data_source is in the key's namespace, so it isn't repeated here.
    """

    model_config = ConfigDict(extra='ignore')


class BaseDataHandler[DataModelT: BaseDataModel, MetadataT: BaseDataMetadata](ABC):
    """Domain logic between a data model and an IO.

    Owns the dataset's identity (DatasetKey), schema validation and metadata schema;
    the IO owns where and how it is stored.
    """

    def __init__(
        self,
        data_model: DataModelT,
        io: BaseIO,
        data_layer: DataLayer | str = DataLayer.CLEANED,
        data_domain: str = 'MARKET_DATA',
    ):
        self._data_model = data_model
        self._io = io
        self._data_layer = DataLayer[str(data_layer).upper()]
        self._data_domain = data_domain.upper()
        self._dataset_key: DatasetKey = self._create_dataset_key()

    @property
    def io(self) -> BaseIO:
        return self._io

    @property
    def data_model(self) -> DataModelT:
        return self._data_model

    @property
    def dataset_key(self) -> DatasetKey:
        return self._dataset_key

    @abstractmethod
    def _create_dataset_key(self) -> DatasetKey:
        """Maps the data model to the dataset it is stored in."""

    @abstractmethod
    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        pass

    @abstractmethod
    def _parse_metadata(self, metadata: Metadata) -> MetadataT:
        """Parses one partition's metadata, as read from the IO, into this handler's metadata class."""

    @abstractmethod
    def write_batch(self, df: pl.DataFrame) -> None:
        pass

    @abstractmethod
    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, MetadataT]]:
        """Reads the data model's data and its per-partition metadata in one IO read."""

    def _write(
        self,
        df: pl.DataFrame,
        partitions: dict[Partition, MetadataT],
        mode: Literal['replace', 'append'] = 'replace',
    ) -> None:
        """Writes df to this handler's dataset, metadata serialized to JSON-safe dicts."""
        self._io.write(
            self._dataset_key,
            df.to_arrow(),
            partitions={partition: md.model_dump(mode='json') for partition, md in partitions.items()},
            mode=mode,
        )

    def _read(
        self, partitions: list[Partition] | None = None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, MetadataT]]:
        """Reads this handler's dataset, metadata parsed by _parse_metadata()."""
        lf, metadata = self._io.read(self._dataset_key, partitions=partitions)
        return lf, {partition: self._parse_metadata(md) for partition, md in metadata.items()}

    def __repr__(self) -> str:
        return f'{type(self).__name__}(data_model={self._data_model}, io={self._io!r}, key={self._dataset_key})'
