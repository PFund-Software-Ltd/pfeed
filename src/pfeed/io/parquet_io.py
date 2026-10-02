from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from pyarrow.parquet import FileMetaData

    from pfeed.io.base_io import DatasetKey, Metadata, Partition, PartitionValue

import datetime
import json
import operator
import os
import posixpath
import uuid
from functools import reduce
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from pfeed.io.base_io import BaseIO, IOCapabilities


class ParquetIO(BaseIO):
    """Stores each partition as one parquet file in a hive layout, metadata in the file's footer.

    Layout: <base_path>/<namespace k=v>/.../<name k=v>/.../<partition_by k=v>/.../part-0.parquet
    e.g. ./data/env=BACKTEST/.../resolution=1t/product=BTC_USDT_PERP/date=2025-01-01/part-0.parquet

    - partition columns are kept inside the file too, so their types survive the round-trip
    - an empty partition is a 0-row file carrying its metadata
    - replace is atomic per partition: write to a temp file, then rename over part-0.parquet
    - no append: a parquet file is immutable, appending means multi-file state, use DeltaLakeIO/DuckLakeIO
    """

    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities(append=False, concurrent_partition_writes=True)
    FILE_NAME: ClassVar[str] = 'part-0.parquet'
    METADATA_KEY: ClassVar[bytes] = b'pfeed_metadata'
    # args set by ParquetIO itself, not overridable via write_options/read_options
    _RESERVED_WRITE_OPTIONS: ClassVar[set[str]] = {'table', 'where', 'filesystem', 'compression'}
    _RESERVED_READ_OPTIONS: ClassVar[set[str]] = {'source', 'hive_partitioning'}

    def __init__(
        self,
        base_path: str,
        compression: str = 'zstd',
        write_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
    ):
        """
        Args:
            base_path: root directory of all datasets. Local paths only for now.
            compression: parquet compression codec.
            write_options: extra kwargs passed to `pyarrow.parquet.write_table`.
            read_options: extra kwargs passed to `polars.scan_parquet`.
        """
        if urlparse(base_path).scheme not in ('', 'file'):
            raise NotImplementedError(f'only local paths are supported for now, got {base_path!r}')
        self._filesystem, self._base_path = pafs.FileSystem.from_uri(os.path.abspath(base_path.removeprefix('file://')))
        self._compression = compression
        self._write_options = write_options or {}
        self._read_options = read_options or {}
        if reserved := self._RESERVED_WRITE_OPTIONS & self._write_options.keys():
            raise ValueError(f'write_options cannot set {reserved}, they are controlled by ParquetIO')
        if reserved := self._RESERVED_READ_OPTIONS & self._read_options.keys():
            raise ValueError(f'read_options cannot set {reserved}, they are controlled by ParquetIO')

    @staticmethod
    def _to_hive_dir(column: str, value: PartitionValue) -> str:
        if isinstance(value, datetime.datetime) or not isinstance(value, (str, int, datetime.date)):
            raise TypeError(f'unsupported partition value {value!r} for {column!r}')
        value = value.isoformat() if isinstance(value, datetime.date) else str(value)
        if not value or '/' in value:
            raise ValueError(f'invalid partition value {value!r} for {column!r}')
        return f'{column}={value}'

    @staticmethod
    def _from_hive_value(value: str, dtype: pa.DataType) -> PartitionValue:
        if pa.types.is_date(dtype):
            return datetime.date.fromisoformat(value)
        if pa.types.is_integer(dtype):
            return int(value)
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
            return value
        raise TypeError(f'unsupported partition column type {dtype}')

    def _dataset_dir(self, key: DatasetKey) -> str:
        dataset_dirs = [self._to_hive_dir(col, value) for col, value in (key.namespace | key.name).items()]
        return posixpath.join(self._base_path, *dataset_dirs)

    def _file_path(self, key: DatasetKey, partition: Partition) -> str:
        dataset_dir = self._dataset_dir(key)
        partition_dirs = [self._to_hive_dir(col, value) for col, value in zip(key.partition_by, partition, strict=True)]
        return posixpath.join(dataset_dir, *partition_dirs, self.FILE_NAME)

    def _find_partitions(self, key: DatasetKey) -> dict[Partition, tuple[str, FileMetaData]]:
        """Finds all partition files of the dataset, parsing partition values back from the hive dirs.

        Returns the footer (FileMetaData) along with the file path, so each file is opened only once.
        """
        dataset_dir = self._dataset_dir(key)
        selector = pafs.FileSelector(dataset_dir, allow_not_found=True, recursive=True)
        file_paths = [
            info.path for info in self._filesystem.get_file_info(selector)
            if info.type == pafs.FileType.File and info.base_name == self.FILE_NAME
        ]
        partitions: dict[Partition, tuple[str, FileMetaData]] = {}
        for file_path in file_paths:
            hive_dirs = file_path.removeprefix(dataset_dir + '/').split('/')[:-1]
            if [d.split('=', 1)[0] for d in hive_dirs] != list(key.partition_by):
                continue  # not a partition of this dataset, e.g. a nested dataset sharing the prefix
            file_metadata = pq.read_metadata(file_path, filesystem=self._filesystem)
            schema = file_metadata.schema.to_arrow_schema()
            partition = tuple(
                self._from_hive_value(d.split('=', 1)[1], schema.field(col).type)
                for d, col in zip(hive_dirs, key.partition_by, strict=True)
            )
            partitions[partition] = (file_path, file_metadata)
        return partitions

    def write(
        self,
        key: DatasetKey,
        data: pa.Table,
        *,
        partitions: dict[Partition, Metadata],
        mode: Literal['replace', 'append'] = 'replace',
    ) -> None:
        if mode == 'append':
            raise NotImplementedError(f'{type(self).__name__} does not support append, use DeltaLakeIO or DuckLakeIO')
        if missing_cols := set(key.partition_by) - set(data.column_names):
            raise ValueError(f'data is missing partition columns {missing_cols}')
        if key.partition_by:
            unique_rows = data.group_by(list(key.partition_by)).aggregate([]).to_pylist()
            data_partitions = {tuple(row[col] for col in key.partition_by) for row in unique_rows}
        else:
            data_partitions = {()} if data.num_rows else set()
        if not_in_partitions := data_partitions - partitions.keys():
            raise ValueError(f'partitions {not_in_partitions} in data are not in `partitions`')
        # serialize all metadata before writing any file, so invalid metadata writes nothing
        dumped_metadata = {partition: self._dump_metadata(md) for partition, md in partitions.items()}

        for partition, partition_metadata in dumped_metadata.items():
            file_path = self._file_path(key, partition)
            if partition in data_partitions and key.partition_by:
                mask = reduce(operator.and_, [
                    pc.field(col) == value for col, value in zip(key.partition_by, partition, strict=True)
                ])
                table = data.filter(mask)
            elif partition in data_partitions:
                table = data
            else:
                table = data.schema.empty_table()
            table = table.replace_schema_metadata({
                **(table.schema.metadata or {}),
                self.METADATA_KEY: partition_metadata,
            })
            # write to a temp file first so readers never see a half-written partition
            partition_dir = file_path.rsplit('/', 1)[0]
            temp_file_path = f'{partition_dir}/.{self.FILE_NAME}.{uuid.uuid4().hex}.tmp'
            self._filesystem.create_dir(partition_dir, recursive=True)
            pq.write_table(
                table,
                temp_file_path,
                filesystem=self._filesystem,
                compression=self._compression,
                **self._write_options,
            )
            self._filesystem.move(temp_file_path, file_path)

    def read(
        self,
        key: DatasetKey,
        *,
        partitions: list[Partition] | None = None,
    ) -> tuple[pl.LazyFrame | None, dict[Partition, Metadata]]:
        if partitions is None:
            files = self._find_partitions(key)
        else:
            files: dict[Partition, tuple[str, FileMetaData]] = {}
            for partition in partitions:
                file_path = self._file_path(key, partition)
                if self._filesystem.get_file_info(file_path).type == pafs.FileType.File:
                    files[partition] = (file_path, pq.read_metadata(file_path, filesystem=self._filesystem))

        metadata: dict[Partition, Metadata] = {}
        non_empty_file_paths: list[str] = []
        for partition, (file_path, file_metadata) in files.items():
            # no metadata = not committed, treat the partition as missing
            if not file_metadata.metadata or self.METADATA_KEY not in file_metadata.metadata:
                continue
            metadata[partition] = json.loads(file_metadata.metadata[self.METADATA_KEY])
            if file_metadata.num_rows:
                non_empty_file_paths.append(file_path)

        data = None
        if non_empty_file_paths:
            data = pl.scan_parquet(non_empty_file_paths, hive_partitioning=False, **self._read_options)
        return data, metadata
