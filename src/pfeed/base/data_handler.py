from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    import pyarrow as pa

    from pfeed.base.data_model import BaseDataModel
    from pfeed.io.base_io import BaseIO, DatasetKey, Metadata as IOMetadata, Partition
    from pfeed.streaming.sink import Sink

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

    # set by each subclass; not a ClassVar only because a ClassVar can't use the type parameter
    Metadata: type[MetadataT]

    def __init__(
        self,
        data_model: DataModelT,
        io: BaseIO,
        data_domain: str,
        data_layer: DataLayer | str = DataLayer.CLEANED,
        sink: Sink | None = None,
    ):
        """
        Args:
            data_domain: the kind of data, e.g. DataCategory.MARKET_DATA, see BaseFeed.data_domain.
            sink: buffers streamed data until a flush is due; None if the handler doesn't write streamed data.
                Streamed data is appended, so `io` must support append.
        """
        if sink is not None and not io.CAPABILITIES.append:
            raise ValueError(f'{io!r} does not support append, it cannot store streamed data')
        self._data_model = data_model
        self._io = io
        self._sink = sink
        self._data_layer = DataLayer[str(data_layer).upper()]
        self._data_domain = data_domain.upper()
        self._dataset_key: DatasetKey = self._create_dataset_key()
        # built up front, so a handler that doesn't support streaming raises here, not at the first flush
        self._streaming_schema: pa.Schema | None = self._build_streaming_schema() if sink is not None else None

    @property
    def io(self) -> BaseIO:
        return self._io

    @property
    def sink(self) -> Sink | None:
        return self._sink

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

    def _create_metadata(self) -> MetadataT:
        """The metadata of a written partition."""
        return self.Metadata()

    def _parse_metadata(self, metadata: IOMetadata) -> MetadataT:
        """Parses one partition's metadata, as read from the IO, into Metadata."""
        return self.Metadata.model_validate(metadata)

    @abstractmethod
    def write_batch(self, df: pl.DataFrame) -> None:
        pass

    @abstractmethod
    def read(self) -> tuple[pl.LazyFrame | None, dict[Partition, MetadataT]]:
        """Reads the data model's data and its per-partition metadata in one IO read."""

    def _build_streaming_schema(self) -> pa.Schema:
        """The schema of the sink's buffered rows; override to support streaming."""
        raise NotImplementedError(f'{type(self).__name__} does not support streaming')

    def _stream_partitions(self, df: pl.DataFrame) -> dict[Partition, MetadataT]:
        """The partitions of the streamed rows in `df`, each with its metadata; override to support streaming."""
        raise NotImplementedError(f'{type(self).__name__} does not support streaming')

    def write_stream(self, msg: Any, **kwargs: Any) -> None:
        """Buffers one streamed message in the sink, flushing it if due; override to support streaming.

        Args:
            kwargs: handler-specific options, see e.g. MarketDataHandler.write_stream().
        """
        raise NotImplementedError(f'{type(self).__name__} does not support streaming')

    def flush(self) -> None:
        """Appends the sink's buffered rows to the IO.

        The rows are only cleared after a successful write, so a failed write is retried at the next flush.
        """
        if self._sink is None or not self._sink.rows:
            return
        import pyarrow as pa

        df = cast('pl.DataFrame', pl.from_arrow(pa.Table.from_pylist(self._sink.rows, schema=self._streaming_schema)))
        self._write(df, self._stream_partitions(df), mode='append')
        self._sink.clear()

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
