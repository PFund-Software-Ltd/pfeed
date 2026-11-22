from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, NamedTuple

if TYPE_CHECKING:
    import os

    from pfeed.io.base_io import DatasetKey, Metadata, Partition

import contextlib
import json
import posixpath
import uuid

import pyarrow.fs as pafs

from pfeed.io.base_io import DatePartition, IOCapabilities, dump_metadata
from pfeed.io.file_utils import (
    resolve_local_base_path,
    retry_on_windows_lock,
    to_hive_dir,
)


class _Committed(NamedTuple):
    """A partition's metadata file, as written by BlobIO.write()."""

    partition: Partition
    file_name: str
    metadata: Metadata


class BlobIO:
    """Reads/writes opaque bytes by DatasetKey, e.g. a model's weights or a component's source code.

    Not a BaseIO: its data is bytes, not a table, pfeed doesn't own the format, the data handler (e.g. pfund's) does.
    Shares BaseIO's other rules: DatasetKey is the identity, metadata is opaque, JSON-safe and the commit marker.

    Layout: <base_path>/<namespace k=v>/.../<name k=v>/.../<partition_by k=v>/.../<file_name> + _pfeed_metadata.json
    e.g. ./data/blob/env=BACKTEST/.../artifact_type=checkpoint/step=3/checkpoint.pth

    - the blob's file name is chosen by the caller (e.g. 'model.joblib', so the file can be opened as is),
        defaults to DEFAULT_FILE_NAME; BlobIO never interprets it, the format stays with the caller

    - partition values are given by the caller, there are no columns to compute them from,
        so partition_by can't have a DatePartition level and values must be str or int
    - the metadata file is the commit marker, it also stores the partition values, so their types survive the round-trip,
        and the blob's file name, so a read finds the blob without being told its name
    - a partition level keeps the type of its values: partitions only differing by type (e.g. 1 vs '1') share a dir,
        so they are the same partition; write() rejects one while the other is stored, but two concurrent writes
        of them aren't covered, same as any concurrent writes to the same partition (see IOCapabilities)
    - replace is atomic per partition: the metadata file is deleted first (the partition is missing), then the blob
        and the metadata are each written to a temp file and renamed into place, metadata last;
        a crash in between leaves the partition missing and the next replace overwrites the leftover blob;
        a replace with another file name deletes the old blob after the commit, a crash before that leaves it orphaned (never read)
    - a read is not a snapshot: a replace between finding a partition's metadata and reading its blob
        can return the new blob with the old metadata, same as ParquetIO,
        or find the blob gone after a replace with another file name, then the partition is missing
    - no append: a blob is replaced as a whole
    """

    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities(
        append=False, concurrent_partition_writes=True
    )
    DEFAULT_DIR_NAME: ClassVar[str] = "blob"
    DEFAULT_FILE_NAME: ClassVar[str] = "part-0.blob"
    METADATA_FILE_NAME: ClassVar[str] = "_pfeed_metadata.json"
    PARTITION_KEY: ClassVar[str] = "partition"
    FILE_NAME_KEY: ClassVar[str] = "file_name"
    METADATA_KEY: ClassVar[str] = "metadata"

    def __init__(self, base_path: str | os.PathLike[str] | None = None):
        """
        Args:
            base_path: root directory of all datasets. Local paths only for now.
                Defaults to <config.data_path>/<DEFAULT_DIR_NAME>.
        """
        self._filesystem, self._base_path = resolve_local_base_path(
            base_path, self.DEFAULT_DIR_NAME
        )

    def __str__(self) -> str:
        return type(self).__name__

    @staticmethod
    def _check_key(key: DatasetKey) -> None:
        if any(isinstance(level, DatePartition) for level in key.partition_by):
            raise ValueError(
                f"{key.partition_by} has a DatePartition level, blobs have no date column to compute it from"
            )

    @staticmethod
    def _check_partition(key: DatasetKey, partition: Partition) -> None:
        if len(partition) != len(key.partition_by):
            raise ValueError(
                f"partition {partition} doesn't match partition_by {key.partition_by}"
            )
        for value in partition:
            # a bool is an int, but would be stored as true/false and break the dir name round-trip
            if isinstance(value, bool) or not isinstance(value, (str, int)):
                raise TypeError(
                    f"unsupported partition value {value!r}, must be str or int"
                )

    @classmethod
    def _check_file_name(cls, file_name: str) -> None:
        # '.' is the prefix of the temp files, see _write_file()
        if (
            not file_name
            or "/" in file_name
            or "\\" in file_name
            or file_name.startswith(".")
            or file_name == cls.METADATA_FILE_NAME
        ):
            raise ValueError(
                f'invalid file name {file_name!r}, it must be non-empty, not contain "/" or "\\", '
                + f'not start with "." and not be {cls.METADATA_FILE_NAME!r}'
            )

    def _dataset_dir(self, key: DatasetKey) -> str:
        dataset_dirs = [
            to_hive_dir(col, value) for col, value in (key.namespace | key.name).items()
        ]
        return posixpath.join(self._base_path, *dataset_dirs)

    def _partition_dir(self, key: DatasetKey, partition: Partition) -> str:
        partition_dirs = [
            to_hive_dir(level, value)
            for level, value in zip(key.partition_by, partition, strict=True)
            if isinstance(level, str)
        ]
        return posixpath.join(self._dataset_dir(key), *partition_dirs)

    def delete_namespace(self, namespace: dict[str, str]) -> None:
        """Deletes every dataset under the namespace, e.g. a whole run. No-op if nothing is stored."""
        namespace_dir = posixpath.join(
            self._base_path,
            *(to_hive_dir(col, value) for col, value in namespace.items()),
        )
        file_info = self._filesystem.get_file_info(namespace_dir)
        if file_info.type == pafs.FileType.NotFound:
            return
        if file_info.type != pafs.FileType.Directory:
            raise RuntimeError(f"{namespace_dir} is not a directory")
        self._filesystem.delete_dir(namespace_dir)

    def _write_file(self, file_path: str, data: bytes) -> None:
        """Writes data to a temp file, then renames it to file_path, so readers never see a half-written file."""
        dir_path = file_path.rsplit("/", 1)[0]
        temp_file_path = (
            f"{dir_path}/.{posixpath.basename(file_path)}.{uuid.uuid4().hex}.tmp"
        )
        self._filesystem.create_dir(dir_path, recursive=True)
        with self._filesystem.open_output_stream(temp_file_path) as f:
            f.write(data)
        retry_on_windows_lock(self._filesystem.move, temp_file_path, file_path)

    def _read_file(self, file_path: str) -> bytes:
        def read() -> bytes:
            with self._filesystem.open_input_stream(file_path) as f:
                return f.read()

        return retry_on_windows_lock(read)

    def _read_committed(self, partition_dir: str) -> _Committed | None:
        """Reads a partition dir's metadata file, None if it has none, i.e. the partition isn't committed.

        A replace deletes the metadata file first (see the class docstring), so it can disappear
        between finding it and reading it; the partition is then missing, as during any replace.
        """
        try:
            committed = json.loads(
                self._read_file(f"{partition_dir}/{self.METADATA_FILE_NAME}")
            )
        except FileNotFoundError:
            return None
        return _Committed(
            partition=tuple(committed[self.PARTITION_KEY]),
            file_name=committed[self.FILE_NAME_KEY],
            metadata=committed[self.METADATA_KEY],
        )

    def _find_partitions(
        self, key: DatasetKey, partitions: list[Partition] | None = None
    ) -> dict[Partition, tuple[str, Metadata]]:
        """Finds the committed partitions of the dataset, with their blobs' file paths and metadata.

        A partition is committed if and only if it has a metadata file (see commit marker).

        Args:
            partitions: partitions to find, only their dirs are looked at; None finds all of the dataset's.
                A partition stored with values of other types (e.g. 1 vs '1') is not found.
        """
        if partitions is not None:
            found: dict[Partition, tuple[str, Metadata]] = {}
            for partition in partitions:
                self._check_partition(key, partition)
                partition_dir = self._partition_dir(key, partition)
                committed = self._read_committed(partition_dir)
                if committed is not None and committed.partition == partition:
                    found[partition] = (
                        f"{partition_dir}/{committed.file_name}",
                        committed.metadata,
                    )
            return found
        dataset_dir = self._dataset_dir(key)
        selector = pafs.FileSelector(dataset_dir, allow_not_found=True, recursive=True)
        found = {}
        for info in self._filesystem.get_file_info(selector):
            if (
                info.type != pafs.FileType.File
                or info.base_name != self.METADATA_FILE_NAME
            ):
                continue
            partition_dir = info.path.rsplit("/", 1)[0]
            hive_dirs = (
                partition_dir.removeprefix(dataset_dir).removeprefix("/").split("/")
                if partition_dir != dataset_dir
                else []
            )
            if [d.split("=", 1)[0] for d in hive_dirs] != list(key.partition_by):
                continue  # not a partition of this dataset, e.g. a nested dataset sharing the prefix
            committed = self._read_committed(partition_dir)
            if committed is not None:
                found[committed.partition] = (
                    f"{partition_dir}/{committed.file_name}",
                    committed.metadata,
                )
        return found

    def write(
        self,
        key: DatasetKey,
        blobs: dict[Partition, tuple[bytes, Metadata]],
        *,
        file_name: str = DEFAULT_FILE_NAME,
    ) -> None:
        """Replaces the partitions in `blobs` with their blobs and metadata, other partitions are untouched.

        A partition level keeps the type of its values: partitions whose values only differ by type
        (e.g. 1 vs '1') share a dir, so writing one while the other is stored raises TypeError.

        Args:
            key: dataset to write to.
            blobs: partition -> (blob, metadata); a blob can be empty (b"").
                For an unpartitioned dataset, the only partition is ().
            file_name: the blobs' file name in their partition dirs, e.g. 'model.joblib'.
        """
        self._check_key(key)
        self._check_file_name(file_name)
        # build all paths and serialize all metadata before writing any file,
        # so an invalid partition value or metadata writes nothing
        # partition dir -> (blob, metadata file content, the replaced blob's file name if it differs)
        writes: dict[str, tuple[bytes, bytes, str | None]] = {}
        dir_partitions: dict[str, Partition] = {}
        for partition, (blob, metadata) in blobs.items():
            self._check_partition(key, partition)
            if not isinstance(blob, bytes):
                raise TypeError(f"blob must be bytes, got {type(blob)}")
            partition_dir = self._partition_dir(key, partition)
            if (other := dir_partitions.get(partition_dir)) is not None:
                raise ValueError(
                    f"partitions {other} and {partition} would be stored in the same dir {partition_dir}"
                )
            dir_partitions[partition_dir] = partition
            committed = self._read_committed(partition_dir)
            if committed is not None and committed.partition != partition:
                raise TypeError(
                    f"partition {partition} would replace the stored partition {committed.partition}, "
                    "their values differ by type"
                )
            writes[partition_dir] = (
                blob,
                dump_metadata(
                    {
                        self.PARTITION_KEY: list(partition),
                        self.FILE_NAME_KEY: file_name,
                        self.METADATA_KEY: metadata,
                    }
                ).encode(),
                committed.file_name
                if committed is not None and committed.file_name != file_name
                else None,
            )
        for partition_dir, (blob, committed, replaced_file_name) in writes.items():
            metadata_file_path = f"{partition_dir}/{self.METADATA_FILE_NAME}"
            # uncommit first, so a crash below never leaves the new blob with the old metadata
            if (
                self._filesystem.get_file_info(metadata_file_path).type
                == pafs.FileType.File
            ):
                retry_on_windows_lock(self._filesystem.delete_file, metadata_file_path)
            self._write_file(f"{partition_dir}/{file_name}", blob)
            self._write_file(metadata_file_path, committed)
            if replaced_file_name is not None:
                with contextlib.suppress(FileNotFoundError):
                    retry_on_windows_lock(
                        self._filesystem.delete_file,
                        f"{partition_dir}/{replaced_file_name}",
                    )

    def read_metadata(
        self, key: DatasetKey, *, partitions: list[Partition] | None = None
    ) -> dict[Partition, Metadata]:
        """Reads the metadata of the existing partitions only, without their blobs, e.g. to check what exists.

        Args:
            key: dataset to read.
            partitions: partitions to read; None reads the whole dataset.

        Returns:
            metadata of the existing partitions; {} if none. Requested partitions not in it are missing.
        """
        self._check_key(key)
        found = self._find_partitions(key, partitions)
        return {partition: metadata for partition, (_, metadata) in found.items()}

    def read(
        self, key: DatasetKey, *, partitions: list[Partition] | None = None
    ) -> tuple[dict[Partition, bytes], dict[Partition, Metadata]]:
        """Reads the blobs and metadata of the existing partitions.

        Unlike BaseIO.read(), the blobs are read eagerly, use read_metadata() if only the metadata is needed.

        Args:
            key: dataset to read.
            partitions: partitions to read; None reads the whole dataset.

        Returns:
            (blobs, metadata), both keyed by the existing partitions; ({}, {}) if none.
            Requested partitions not in them are missing.
        """
        self._check_key(key)
        blobs: dict[Partition, bytes] = {}
        metadata: dict[Partition, Metadata] = {}
        for partition, (file_path, md) in self._find_partitions(
            key, partitions
        ).items():
            try:
                blobs[partition] = self._read_file(file_path)
            except FileNotFoundError:
                continue  # deleted by a replace with another file name, see the class docstring
            metadata[partition] = md
        return blobs, metadata
