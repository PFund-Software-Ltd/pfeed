from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from pyarrow.parquet import FileMetaData

    from pfeed.io.base_io import (
        DatasetKey,
        Metadata,
        Partition,
        PartitionLevel,
        PartitionValue,
    )

import datetime
import json
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

from pfeed.io.base_io import BaseIO, DatePartition, IOCapabilities


class ParquetIO(BaseIO):
    """Stores each partition as one parquet file in a hive layout, metadata in the file's footer.

    Layout: <base_path>/<namespace k=v>/.../<name k=v>/.../<partition_by k=v>/.../part-0.parquet
    e.g. ./data/env=BACKTEST/.../resolution=1t/product=BTC_USDT_PERP/year=2025/month=01/day=01/part-0.parquet

    - a column level is one dir `col=value`; a DatePartition level is three dirs `year=YYYY/month=MM/day=DD`
    - partition columns are kept inside the file too, so their types survive the round-trip;
        a DatePartition's date is only in the dirs, it is computed from its column, not stored
    - an empty partition is a 0-row file carrying its metadata
    - replace is atomic per partition: write to a temp file, then rename over part-0.parquet
    - no append: a parquet file is immutable, appending means multi-file state, use DeltaLakeIO/DuckLakeIO
    """

    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities(append=False, concurrent_partition_writes=True)
    DEFAULT_DIR_NAME: ClassVar[str] = 'parquet'
    FILE_NAME: ClassVar[str] = 'part-0.parquet'
    METADATA_KEY: ClassVar[bytes] = b'pfeed_metadata'
    # args set by ParquetIO itself, not overridable via write_options/read_options
    _RESERVED_WRITE_OPTIONS: ClassVar[set[str]] = {'table', 'where', 'filesystem', 'compression'}
    _RESERVED_READ_OPTIONS: ClassVar[set[str]] = {'source', 'hive_partitioning', 'schema', 'missing_columns'}

    def __init__(
        self,
        base_path: str | None = None,
        compression: str = 'zstd',
        write_options: dict[str, Any] | None = None,
        read_options: dict[str, Any] | None = None,
    ):
        """
        Args:
            base_path: root directory of all datasets. Local paths only for now.
                Defaults to <config.data_path>/<DEFAULT_DIR_NAME>.
            compression: parquet compression codec.
            write_options: extra kwargs passed to `pyarrow.parquet.write_table`.
            read_options: extra kwargs passed to `polars.scan_parquet`.
        """
        if base_path is None:
            from pfeed.config import get_config

            base_path = str(get_config().data_path / self.DEFAULT_DIR_NAME)
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

    @staticmethod
    def _level_dir_names(level: PartitionLevel) -> list[str]:
        return ['year', 'month', 'day'] if isinstance(level, DatePartition) else [level]

    @classmethod
    def _partition_dir_names(cls, key: DatasetKey) -> list[str]:
        """The hive dir names of the partition levels, raising ValueError if two levels share a dir name."""
        dir_names = [name for level in key.partition_by for name in cls._level_dir_names(level)]
        if len(set(dir_names)) != len(dir_names):
            raise ValueError(f'partition_by {key.partition_by} has duplicate dir names {dir_names}')
        return dir_names

    @classmethod
    def _to_partition_dirs(cls, level: PartitionLevel, value: PartitionValue) -> list[str]:
        if isinstance(level, DatePartition):
            if isinstance(value, datetime.datetime) or not isinstance(value, datetime.date):
                raise TypeError(f'{level} partition value must be a date, got {value!r}')
            return [f'year={value.year:04d}', f'month={value.month:02d}', f'day={value.day:02d}']
        return [cls._to_hive_dir(level, value)]

    def _dataset_dir(self, key: DatasetKey) -> str:
        dataset_dirs = [self._to_hive_dir(col, value) for col, value in (key.namespace | key.name).items()]
        return posixpath.join(self._base_path, *dataset_dirs)

    def _file_path(self, key: DatasetKey, partition: Partition) -> str:
        dataset_dir = self._dataset_dir(key)
        partition_dirs = [
            partition_dir
            for level, value in zip(key.partition_by, partition, strict=True)
            for partition_dir in self._to_partition_dirs(level, value)
        ]
        return posixpath.join(dataset_dir, *partition_dirs, self.FILE_NAME)

    def _find_partitions(self, key: DatasetKey) -> dict[Partition, tuple[str, FileMetaData]]:
        """Finds all committed partition files of the dataset, parsing partition values back from the hive dirs.

        Returns the footer (FileMetaData) along with the file path, so each file is opened only once.
        A file without metadata isn't committed (see commit marker), so its partition is treated as missing.
        """
        dataset_dir = self._dataset_dir(key)
        dir_names = self._partition_dir_names(key)
        selector = pafs.FileSelector(dataset_dir, allow_not_found=True, recursive=True)
        file_paths = [
            info.path for info in self._filesystem.get_file_info(selector)
            if info.type == pafs.FileType.File and info.base_name == self.FILE_NAME
        ]
        partitions: dict[Partition, tuple[str, FileMetaData]] = {}
        for file_path in file_paths:
            hive_dirs = file_path.removeprefix(dataset_dir + '/').split('/')[:-1]
            if [d.split('=', 1)[0] for d in hive_dirs] != dir_names:
                continue  # not a partition of this dataset, e.g. a nested dataset sharing the prefix
            file_metadata = pq.read_metadata(file_path, filesystem=self._filesystem)
            if not file_metadata.metadata or self.METADATA_KEY not in file_metadata.metadata:
                continue
            schema = file_metadata.schema.to_arrow_schema()
            values = iter(d.split('=', 1)[1] for d in hive_dirs)
            partition = tuple(
                datetime.date(int(next(values)), int(next(values)), int(next(values)))
                if isinstance(level, DatePartition)
                else self._from_hive_value(next(values), schema.field(level).type)
                for level in key.partition_by
            )
            partitions[partition] = (file_path, file_metadata)
        return partitions

    @staticmethod
    def _dataset_schema(schemas: list[pa.Schema]) -> pa.Schema:
        """Unions the columns of all partition files, since each file only has the columns it was written with.

        Raises TypeError if a column has different types across schemas.
        """
        try:
            return pa.unify_schemas(schemas)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            raise TypeError(f'a column has different types across partitions: {e}') from e

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
        data_partitions = self._data_partitions(key, data, partitions)
        partition_value_arrays = self._partition_value_arrays(key, data)
        # build all paths and serialize all metadata before writing any file,
        # so an invalid partition value or metadata writes nothing
        file_paths = {partition: self._file_path(key, partition) for partition in partitions}
        dumped_metadata = {partition: self._dump_metadata(md) for partition, md in partitions.items()}
        # schema drift: new/missing columns are fine, a column changing type isn't
        existing_files = self._find_partitions(key)
        self._dataset_schema([fm.schema.to_arrow_schema() for _, fm in existing_files.values()] + [data.schema])

        for partition, partition_metadata in dumped_metadata.items():
            file_path = file_paths[partition]
            if partition in data_partitions and key.partition_by:
                mask = reduce(pc.and_, [
                    pc.equal(array, pa.scalar(value, type=array.type))
                    for array, value in zip(partition_value_arrays, partition, strict=True)
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
        # all files are needed even for a subset, since the dataset's columns are the union of all files (schema drift)
        all_files = self._find_partitions(key)
        if partitions is None:
            files = all_files
        else:
            files = {partition: all_files[partition] for partition in partitions if partition in all_files}

        metadata: dict[Partition, Metadata] = {}
        non_empty_file_paths: list[str] = []
        for partition, (file_path, file_metadata) in files.items():
            metadata[partition] = json.loads(file_metadata.metadata[self.METADATA_KEY])
            if file_metadata.num_rows:
                non_empty_file_paths.append(file_path)

        data = None
        if non_empty_file_paths:
            schema = self._dataset_schema([fm.schema.to_arrow_schema() for _, fm in all_files.values()])
            data = pl.scan_parquet(
                non_empty_file_paths,
                hive_partitioning=False,
                schema=pl.DataFrame(schema.empty_table()).schema,
                missing_columns='insert',  # null for columns a file doesn't have
                **self._read_options,
            )
        return data, metadata
