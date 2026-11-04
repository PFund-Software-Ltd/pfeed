from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    import pyarrow as pa

    from pfeed.base.data_model import BaseDataModel
    from pfeed.io.base_io import BaseIO, DatasetKey, Metadata as IOMetadata, Partition
    from pfeed.streaming.buffer import StreamBuffer

import logging
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
    # consecutive failed due flushes tolerated before flush_if_due() raises, so a persistent error still fails the stream
    MAX_FAILED_FLUSHES: ClassVar[int] = 3

    def __init__(
        self,
        data_model: DataModelT,
        io: BaseIO,
        data_domain: str,
        data_layer: DataLayer | str = DataLayer.CLEANED,
        is_streaming: bool = False,
    ):
        """
        Args:
            data_domain: the kind of data, e.g. DataCategory.MARKET_DATA, see BaseFeed.data_domain.
            is_streaming: whether the dataset holds streamed data, which is stored with a different schema,
                e.g. partitioned by start_ts instead of date, see _get_date_col().
        """
        self._logger = logging.getLogger(f"pfeed.{data_model.data_source.lower()}")
        self._data_model = data_model
        self._io = io
        self._data_layer = DataLayer[str(data_layer).upper()]
        self._data_domain = data_domain.upper()
        self._is_streaming = is_streaming
        self._dataset_key: DatasetKey = self._create_dataset_key()
        self._stream_buffer: StreamBuffer | None = None
        self._failed_flushes = 0

    @property
    def name(self) -> str:
        return ".".join([
            self._data_model.data_source,
            self._data_domain,
            str(self._data_layer),
            str(self._io),
        ])

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

    def setup_streaming(self, flush_interval: float):
        from pfeed.streaming.buffer import StreamBuffer

        if not self._is_streaming:
            raise ValueError(f'{self!r} is not for streamed data, create it with is_streaming=True')
        if self._data_layer == DataLayer.RAW:
            raise NotImplementedError('storing streamed raw data is not supported yet')
        if not self._io.CAPABILITIES.append:
            raise ValueError(f'{self._io!r} does not support append, it cannot store streamed data')
        # built up front, so a handler that doesn't support streaming raises here, not at the first flush
        self._stream_buffer = StreamBuffer(
            schema=self._build_streaming_schema(),
            flush_interval=flush_interval
        )

    def teardown_streaming(self) -> None:
        """Ends the streaming set up by setup_streaming(): flushes the buffered rows, then drops the buffer, so write_stream() raises after it.

        A failed flush raises and keeps the buffer with its rows.
        """
        if self._stream_buffer is None:
            return
        self.flush()
        self._stream_buffer = None

    def flush(self) -> None:
        """Appends the stream buffer's rows to the IO.

        A failed write raises and puts the rows back in the buffer, see flush_if_due() for retrying it.
        """
        if self._stream_buffer is None or not self._stream_buffer.rows:
            return
        rows = self._stream_buffer.drain()
        try:
            df = self._stream_buffer.to_dataframe(rows)
            self._write(df, self._stream_partitions(df), mode='append')
        except Exception:
            self._stream_buffer.restore(rows)
            raise
        self._logger.debug(f"flushed {len(rows)} rows to {self!r}")

    def is_flush_due(self) -> bool:
        return self._stream_buffer is not None and self._stream_buffer.is_due()

    def flush_if_due(self) -> None:
        """Flushes if due; a failed write is logged and retried at the next flush, e.g. after a transient "database is locked".

        Raises after MAX_FAILED_FLUSHES consecutive failures, so a persistent error (e.g. a full disk) doesn't grow the buffer forever.
        """
        if not self.is_flush_due():
            return
        try:
            self.flush()
        except Exception:
            self._failed_flushes += 1
            if self._failed_flushes >= self.MAX_FAILED_FLUSHES:
                raise
            self._logger.exception(
                f"{self!r} flush failed ({self._failed_flushes}/{self.MAX_FAILED_FLUSHES}), retrying at the next flush:"
            )
        else:
            self._failed_flushes = 0

    def write_stream(self, msg: Any) -> None:
        """Buffers one streamed message in the sink, flushing it if due; override to support streaming.

        Args:
            kwargs: handler-specific options, see e.g. MarketDataHandler.write_stream().
        """
        raise NotImplementedError(f'{type(self).__name__} does not support streaming')

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
