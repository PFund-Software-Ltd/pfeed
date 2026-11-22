from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, NamedTuple

if TYPE_CHECKING:
    import os

    from pfeed.io.base_io import DatasetKey, Metadata, Partition

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


class _Manifest(NamedTuple):
    """A partition's metadata file, as written by BlobIO.write(): the committed files of the partition."""

    partition: Partition
    # file name -> its metadata
    files: dict[str, Metadata]


class BlobIO:
    """Reads/writes opaque bytes by DatasetKey, e.g. a model's weights or a component's source code.

    Not a BaseIO: its data is bytes, not a table, pfeed doesn't own the format, the data handler (e.g. pfund's) does.
    Shares BaseIO's other rules: DatasetKey is the identity, metadata is opaque, JSON-safe and the commit marker.

    Layout: <base_path>/<namespace>/.../<name>/.../<partition_by k=v>/.../<file_name>(s) + _pfeed_metadata.json
    e.g. ./data/blob/env=BACKTEST/.../artifact_type=checkpoint/step=3/checkpoint.pth
    - namespace and name levels are hive dirs (k=v) by default, or plain values (e.g. BACKTEST/) with hive_dataset_dirs=False;
        partition levels are always hive dirs, since a read finds the partitions of a dataset by their level names

    - a partition holds one or more blobs, each under a file name chosen by the caller
        (e.g. 'model.joblib', so the file can be opened as is), DEFAULT_FILE_NAME by default;
        BlobIO never interprets it, the format stays with the caller
    - the partition's metadata file (_pfeed_metadata.json) is its manifest and commit marker:
        it lists the partition's committed files with their metadata, a blob not in it isn't committed (never read);
        it also stores the partition values, so their types survive the round-trip
    - a write merges into the manifest: a new file name is added, an existing one is replaced, the other files are untouched

    - partition values are given by the caller, there are no columns to compute them from,
        so partition_by can't have a DatePartition level and values must be str or int
    - a partition level keeps the type of its values: partitions only differing by type (e.g. 1 vs '1') share a dir,
        so they are the same partition; write() rejects one while the other is stored
    - concurrent writes to the same partition aren't covered (see IOCapabilities), even with different file names:
        each rewrites the manifest, so one can drop the other's entry; writes to different partitions are independent
    - replace is atomic per file: the file's entry is removed from the manifest first (the file is missing),
        then the blob and the manifest are each written to a temp file and renamed into place, manifest last;
        a crash in between leaves the file missing and the next replace overwrites the leftover blob
    - a read is not a snapshot: a replace between reading the manifest and the blob
        can return the new blob with the old metadata, same as ParquetIO
    - no append: a blob is replaced as a whole
    """

    CAPABILITIES: ClassVar[IOCapabilities] = IOCapabilities(
        append=False, concurrent_partition_writes=True
    )
    DEFAULT_DIR_NAME: ClassVar[str] = "blob"
    DEFAULT_FILE_NAME: ClassVar[str] = "part-0.blob"
    METADATA_FILE_NAME: ClassVar[str] = "_pfeed_metadata.json"
    PARTITION_KEY: ClassVar[str] = "partition"
    FILES_KEY: ClassVar[str] = "files"

    def __init__(
        self,
        base_path: str | os.PathLike[str] | None = None,
        *,
        hive_dataset_dirs: bool = True,
    ):
        """
        Args:
            base_path: root directory of all datasets. Local paths only for now.
                Defaults to <config.data_path>/<DEFAULT_DIR_NAME>.
            hive_dataset_dirs: whether the namespace and name levels are hive dirs (e.g. env=BACKTEST/),
                or plain values (e.g. BACKTEST/); partition levels are always hive dirs.
        """
        self._filesystem, self._base_path = resolve_local_base_path(
            base_path, self.DEFAULT_DIR_NAME
        )
        self._hive_dataset_dirs = hive_dataset_dirs

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

    def _dataset_level_dir(self, column: str, value: str) -> str:
        """The dir of a namespace/name level: a hive dir (k=v), or its plain value if not hive_dataset_dirs."""
        # to_hive_dir() validates the value (non-empty, no '/') in both cases
        hive_dir = to_hive_dir(column, value)
        if self._hive_dataset_dirs:
            return hive_dir
        if value in (".", ".."):
            raise ValueError(f"invalid value {value!r} for {column!r}, it isn't a dir")
        return hive_dir.removeprefix(f"{column}=")

    def _dataset_dir(self, key: DatasetKey) -> str:
        dataset_dirs = [
            self._dataset_level_dir(col, value)
            for col, value in (key.namespace | key.name).items()
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
            *(self._dataset_level_dir(col, value) for col, value in namespace.items()),
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

    def _read_manifest(self, partition_dir: str) -> _Manifest | None:
        """Reads a partition dir's manifest, None if it has none, i.e. the partition has no committed files.

        A replace of the partition's last file deletes the manifest first (see the class docstring),
        so it can disappear between finding it and reading it; the partition is then missing, as during any replace.
        """
        try:
            manifest = json.loads(
                self._read_file(f"{partition_dir}/{self.METADATA_FILE_NAME}")
            )
        except FileNotFoundError:
            return None
        return _Manifest(
            partition=tuple(manifest[self.PARTITION_KEY]),
            files=manifest[self.FILES_KEY],
        )

    def _write_manifest(
        self, partition_dir: str, partition: Partition, files: dict[str, Metadata]
    ) -> None:
        """Writes the partition's manifest, or deletes it if no files are left, i.e. the partition is missing."""
        manifest_path = f"{partition_dir}/{self.METADATA_FILE_NAME}"
        if files:
            manifest = {self.PARTITION_KEY: list(partition), self.FILES_KEY: files}
            self._write_file(manifest_path, dump_metadata(manifest).encode())
        elif self._filesystem.get_file_info(manifest_path).type == pafs.FileType.File:
            retry_on_windows_lock(self._filesystem.delete_file, manifest_path)

    def _find_partitions(
        self,
        key: DatasetKey,
        file_name: str,
        partitions: list[Partition] | None = None,
    ) -> dict[Partition, tuple[str, Metadata]]:
        """Finds the partitions of the dataset with file_name committed, with the file's path and metadata.

        A file is committed if and only if the partition's manifest lists it (see commit marker).

        Args:
            file_name: the file to find in each partition, partitions without it are missing.
            partitions: partitions to find, only their dirs are looked at; None finds all of the dataset's.
                A partition stored with values of other types (e.g. 1 vs '1') is not found.
        """
        found: dict[Partition, tuple[str, Metadata]] = {}

        def add(partition_dir: str, manifest: _Manifest | None) -> None:
            if manifest is not None and file_name in manifest.files:
                found[manifest.partition] = (
                    f"{partition_dir}/{file_name}",
                    manifest.files[file_name],
                )

        if partitions is not None:
            for partition in partitions:
                self._check_partition(key, partition)
                partition_dir = self._partition_dir(key, partition)
                manifest = self._read_manifest(partition_dir)
                if manifest is not None and manifest.partition == partition:
                    add(partition_dir, manifest)
            return found
        dataset_dir = self._dataset_dir(key)
        selector = pafs.FileSelector(dataset_dir, allow_not_found=True, recursive=True)
        for info in self._filesystem.get_file_info(selector):
            if (
                info.type != pafs.FileType.File
                or info.base_name != self.METADATA_FILE_NAME
            ):
                continue
            partition_dir = info.path.rsplit("/", 1)[0]
            partition_dirs = (
                partition_dir.removeprefix(dataset_dir).removeprefix("/").split("/")
                if partition_dir != dataset_dir
                else []
            )
            # not a partition of this dataset, e.g. a nested dataset sharing the prefix
            if len(partition_dirs) != len(key.partition_by) or not all(
                d.startswith(f"{level}=")
                for d, level in zip(partition_dirs, key.partition_by, strict=True)
            ):
                continue
            add(partition_dir, self._read_manifest(partition_dir))
        return found

    def write(
        self,
        key: DatasetKey,
        blobs: dict[Partition, tuple[bytes, Metadata]],
        *,
        file_name: str = DEFAULT_FILE_NAME,
    ) -> None:
        """Writes file_name with its blob and metadata in each partition of `blobs`, merged into the partition's manifest.

        A new file name is added to the partition, an existing one is replaced;
        the partition's other files and the other partitions are untouched.
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
        # check all partitions and metadata before writing any file,
        # so an invalid partition value or metadata writes nothing
        # partition dir -> (partition, blob, metadata, the stored files)
        writes: dict[str, tuple[Partition, bytes, Metadata, dict[str, Metadata]]] = {}
        for partition, (blob, metadata) in blobs.items():
            self._check_partition(key, partition)
            if not isinstance(blob, bytes):
                raise TypeError(f"blob must be bytes, got {type(blob)}")
            dump_metadata(metadata)
            partition_dir = self._partition_dir(key, partition)
            if (other := writes.get(partition_dir)) is not None:
                raise ValueError(
                    f"partitions {other[0]} and {partition} would be stored in the same dir {partition_dir}"
                )
            manifest = self._read_manifest(partition_dir)
            if manifest is not None and manifest.partition != partition:
                raise TypeError(
                    f"partition {partition} would replace the stored partition {manifest.partition}, "
                    "their values differ by type"
                )
            files = dict(manifest.files) if manifest is not None else {}
            writes[partition_dir] = (partition, blob, metadata, files)
        for partition_dir, (partition, blob, metadata, files) in writes.items():
            # uncommit a replaced file first, so a crash below never leaves the new blob with the old metadata
            if files.pop(file_name, None) is not None:
                self._write_manifest(partition_dir, partition, files)
            self._write_file(f"{partition_dir}/{file_name}", blob)
            self._write_manifest(
                partition_dir, partition, files | {file_name: metadata}
            )

    def read_metadata(
        self,
        key: DatasetKey,
        *,
        partitions: list[Partition] | None = None,
        file_name: str = DEFAULT_FILE_NAME,
    ) -> dict[Partition, Metadata]:
        """Reads file_name's metadata in the existing partitions only, without the blobs, e.g. to check what exists.

        Args:
            key: dataset to read.
            partitions: partitions to read; None reads the whole dataset.
            file_name: the file to read in each partition, the one passed to write().

        Returns:
            metadata of the existing partitions with file_name; {} if none. Requested partitions not in it are missing.
        """
        self._check_key(key)
        self._check_file_name(file_name)
        found = self._find_partitions(key, file_name, partitions)
        return {partition: metadata for partition, (_, metadata) in found.items()}

    def read(
        self,
        key: DatasetKey,
        *,
        partitions: list[Partition] | None = None,
        file_name: str = DEFAULT_FILE_NAME,
    ) -> tuple[dict[Partition, bytes], dict[Partition, Metadata]]:
        """Reads file_name's blob and metadata in the existing partitions.

        Unlike BaseIO.read(), the blobs are read eagerly, use read_metadata() if only the metadata is needed.

        Args:
            key: dataset to read.
            partitions: partitions to read; None reads the whole dataset.
            file_name: the file to read in each partition, the one passed to write().

        Returns:
            (blobs, metadata), both keyed by the existing partitions with file_name; ({}, {}) if none.
            Requested partitions not in them are missing.
        """
        self._check_key(key)
        self._check_file_name(file_name)
        blobs: dict[Partition, bytes] = {}
        metadata: dict[Partition, Metadata] = {}
        for partition, (file_path, md) in self._find_partitions(
            key, file_name, partitions
        ).items():
            try:
                blobs[partition] = self._read_file(file_path)
            except FileNotFoundError:
                continue  # e.g. deleted by hand, the manifest still lists it
            metadata[partition] = md
        return blobs, metadata
