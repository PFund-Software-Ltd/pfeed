"""BlobIO tests: bytes by DatasetKey, with BaseIO's metadata and commit-marker rules."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

import datetime

import pytest

from pfeed.io.base_io import DatasetKey, DatePartition
from pfeed.io.blob_io import BlobIO

NAMESPACE = {"env": "BACKTEST", "project_name": "proj"}
NAME = {"component_id": "Model-abc", "artifact_type": "checkpoint"}
# where BlobIO puts a dataset with NAMESPACE and NAME, relative to base_path
DATASET_DIR = (
    "env=BACKTEST/project_name=proj/component_id=Model-abc/artifact_type=checkpoint"
)
KEY = DatasetKey(namespace=NAMESPACE, name=NAME)
STEP_KEY = DatasetKey(namespace=NAMESPACE, name=NAME, partition_by=("step",))


@pytest.fixture
def io(tmp_path: Path) -> BlobIO:
    return BlobIO(base_path=str(tmp_path))


def test_non_local_base_path_raises():
    """Only local paths are supported for now, same as the other IOs."""
    with pytest.raises(NotImplementedError):
        BlobIO(base_path="s3://bucket/data")


def test_unpartitioned_round_trip(io: BlobIO, tmp_path: Path):
    """An unpartitioned dataset has the one partition (), its blob is stored verbatim next to its metadata."""
    io.write(KEY, {(): (b"weights", {"extension": ".safetensors"})})

    assert (
        tmp_path / DATASET_DIR / BlobIO.DEFAULT_FILE_NAME
    ).read_bytes() == b"weights"
    assert (tmp_path / DATASET_DIR / BlobIO.METADATA_FILE_NAME).is_file()
    assert io.read(KEY) == ({(): b"weights"}, {(): {"extension": ".safetensors"}})
    assert io.read_metadata(KEY) == {(): {"extension": ".safetensors"}}


def test_missing_dataset(io: BlobIO):
    assert io.read(KEY) == ({}, {})
    assert io.read(STEP_KEY, partitions=[(1,)]) == ({}, {})
    assert io.read_metadata(KEY) == {}


def test_partitions(io: BlobIO, tmp_path: Path):
    """Each partition is its own dir; partition values keep their type (int) and replace leaves other partitions untouched."""
    io.write(STEP_KEY, {(0,): (b"s0", {"v": 1}), (1,): (b"s1", {"v": 1})})
    assert (
        tmp_path / DATASET_DIR / "step=1" / BlobIO.DEFAULT_FILE_NAME
    ).read_bytes() == b"s1"

    io.write(STEP_KEY, {(1,): (b"s1-new", {"v": 2})})

    assert io.read(STEP_KEY) == (
        {(0,): b"s0", (1,): b"s1-new"},
        {(0,): {"v": 1}, (1,): {"v": 2}},
    )
    assert io.read(STEP_KEY, partitions=[(1,), (2,)]) == (
        {(1,): b"s1-new"},
        {(1,): {"v": 2}},
    )


def test_empty_blob(io: BlobIO):
    """An empty blob is a valid, existing partition."""
    io.write(KEY, {(): (b"", {})})

    assert io.read(KEY) == ({(): b""}, {(): {}})


def test_blob_without_metadata_is_missing(io: BlobIO, tmp_path: Path):
    """A blob without its metadata file isn't committed (e.g. a crash before the metadata was written)."""
    dataset_dir = tmp_path / DATASET_DIR
    dataset_dir.mkdir(parents=True)
    (dataset_dir / BlobIO.DEFAULT_FILE_NAME).write_bytes(b"leftover")

    assert io.read(KEY) == ({}, {})


def test_crash_during_replace_leaves_partition_missing(
    io: BlobIO, mocker: MockerFixture
):
    """Replace uncommits the partition before writing the new blob, so a crash never pairs the new blob with the old metadata."""
    io.write(KEY, {(): (b"old", {"v": 1})})
    write_file = io._write_file

    def crash_on_metadata(file_path: str, data: bytes) -> None:
        if file_path.endswith(BlobIO.METADATA_FILE_NAME):
            raise RuntimeError("crash")
        write_file(file_path, data)

    mocker.patch.object(io, "_write_file", side_effect=crash_on_metadata)
    with pytest.raises(RuntimeError):
        io.write(KEY, {(): (b"new", {"v": 2})})

    assert io.read(KEY) == ({}, {})


@pytest.mark.parametrize(
    "partition, error",
    [
        (("1",), None),  # str is fine
        ((1.5,), TypeError),
        ((True,), TypeError),  # a bool is an int, but wouldn't round-trip
        ((datetime.date(2025, 1, 1),), TypeError),
        (("",), ValueError),  # would be dir 'step='
        (("a/b",), ValueError),  # would be two dirs
        ((1, 2), ValueError),  # doesn't match partition_by
    ],
)
def test_invalid_partition_raises_and_writes_nothing(
    io: BlobIO, partition: tuple, error: type[Exception] | None
):
    """An invalid partition raises before any file is written, even if a valid partition comes first."""
    partitions = {(0,): (b"ok", {}), partition: (b"bad", {})}
    if error is None:
        io.write(STEP_KEY, partitions)
        return
    with pytest.raises(error):
        io.write(STEP_KEY, partitions)
    assert io.read(STEP_KEY) == ({}, {})


def test_non_json_safe_metadata_raises(io: BlobIO):
    with pytest.raises(TypeError):
        io.write(KEY, {(): (b"x", {"date": datetime.date(2025, 1, 1)})})
    assert io.read(KEY) == ({}, {})


def test_non_bytes_blob_raises(io: BlobIO):
    with pytest.raises(TypeError):
        io.write(KEY, {(): ("text", {})})  # ty: ignore[invalid-argument-type]


def test_date_partition_raises(io: BlobIO):
    """Blobs have no date column to compute a DatePartition from."""
    key = DatasetKey(
        namespace=NAMESPACE, name=NAME, partition_by=(DatePartition("date"),)
    )
    with pytest.raises(ValueError):
        io.write(key, {})
    with pytest.raises(ValueError):
        io.read(key)


def test_nested_dataset_is_not_a_partition(io: BlobIO):
    """A dataset whose dir is under another dataset's dir doesn't leak into the outer dataset's partitions."""
    nested = DatasetKey(namespace=NAMESPACE, name={**NAME, "extra": "x"})
    io.write(nested, {(): (b"nested", {})})

    assert io.read(KEY) == ({}, {})


def test_partitions_differing_by_type_in_one_write_raise(io: BlobIO):
    """(1,) and ('1',) would share the dir step=1, so one would silently overwrite the other."""
    with pytest.raises(ValueError):
        io.write(STEP_KEY, {(1,): (b"int", {}), ("1",): (b"str", {})})
    assert io.read(STEP_KEY) == ({}, {})


def test_replacing_a_partition_of_another_type_raises(io: BlobIO):
    """A partition level keeps the type of its stored values, a write differing by type can't replace them."""
    io.write(STEP_KEY, {(1,): (b"int", {"v": 1})})

    with pytest.raises(TypeError):
        io.write(STEP_KEY, {("1",): (b"str", {"v": 2})})

    assert io.read(STEP_KEY) == ({(1,): b"int"}, {(1,): {"v": 1}})
    # the same dir, but not the requested partition
    assert io.read(STEP_KEY, partitions=[("1",)]) == ({}, {})


def test_metadata_disappearing_during_read_is_missing(
    io: BlobIO, mocker: MockerFixture
):
    """A replace deletes the metadata first, a read finding it just before is left with a missing partition, not an error."""
    io.write(STEP_KEY, {(0,): (b"s0", {}), (1,): (b"s1", {})})
    read_file = io._read_file

    def replaced_during_read(file_path: str) -> bytes:
        if file_path.endswith(f"step=1/{BlobIO.METADATA_FILE_NAME}"):
            raise FileNotFoundError(file_path)
        return read_file(file_path)

    mocker.patch.object(io, "_read_file", side_effect=replaced_during_read)

    assert io.read(STEP_KEY) == ({(0,): b"s0"}, {(0,): {}})
    assert io.read(STEP_KEY, partitions=[(1,)]) == ({}, {})
    assert io.read_metadata(STEP_KEY) == {(0,): {}}


def test_filtered_read_only_reads_requested_partitions(
    io: BlobIO, mocker: MockerFixture
):
    """A filtered read doesn't touch other partitions, e.g. one in the middle of a replace."""
    io.write(STEP_KEY, {(0,): (b"s0", {}), (1,): (b"s1", {})})
    read_file = io._read_file

    def fail_on_step_1(file_path: str) -> bytes:
        if "step=1" in file_path:
            raise RuntimeError("must not be read")
        return read_file(file_path)

    mocker.patch.object(io, "_read_file", side_effect=fail_on_step_1)

    assert io.read(STEP_KEY, partitions=[(0,)]) == ({(0,): b"s0"}, {(0,): {}})


def test_file_name(io: BlobIO, tmp_path: Path):
    """The blob is stored under the caller's file name, so it can be opened as is, and read back without passing it."""
    io.write(STEP_KEY, {(0,): (b"ckpt", {})}, file_name="checkpoint.pth")

    step_dir = tmp_path / DATASET_DIR / "step=0"
    assert sorted(p.name for p in step_dir.iterdir()) == [
        BlobIO.METADATA_FILE_NAME,
        "checkpoint.pth",
    ]
    assert (step_dir / "checkpoint.pth").read_bytes() == b"ckpt"
    assert io.read(STEP_KEY) == ({(0,): b"ckpt"}, {(0,): {}})


def test_replace_with_another_file_name_deletes_the_old_blob(
    io: BlobIO, tmp_path: Path
):
    """e.g. a model switching from joblib to safetensors leaves no stale model.joblib behind."""
    io.write(KEY, {(): (b"old", {})}, file_name="model.joblib")
    io.write(KEY, {(): (b"new", {})}, file_name="model.safetensors")

    assert not (tmp_path / DATASET_DIR / "model.joblib").exists()
    assert io.read(KEY) == ({(): b"new"}, {(): {}})


def test_blob_deleted_during_read_is_missing(io: BlobIO, mocker: MockerFixture):
    """A replace with another file name deletes the old blob after its commit,
    a read that found the old metadata just before is left with a missing partition, not an error.
    """
    io.write(STEP_KEY, {(0,): (b"s0", {}), (1,): (b"s1", {})})
    read_file = io._read_file

    def deleted_during_read(file_path: str) -> bytes:
        if file_path.endswith(f"step=1/{BlobIO.DEFAULT_FILE_NAME}"):
            raise FileNotFoundError(file_path)
        return read_file(file_path)

    mocker.patch.object(io, "_read_file", side_effect=deleted_during_read)

    assert io.read(STEP_KEY) == ({(0,): b"s0"}, {(0,): {}})


@pytest.mark.parametrize(
    "file_name",
    ["", "a/b", "a\\b", ".hidden", BlobIO.METADATA_FILE_NAME],
    ids=["empty", "slash", "backslash", "dot", "metadata"],
)
def test_invalid_file_name_raises(io: BlobIO, file_name: str):
    with pytest.raises(ValueError):
        io.write(KEY, {(): (b"x", {})}, file_name=file_name)
    assert io.read(KEY) == ({}, {})


def test_delete_namespace(io: BlobIO, tmp_path: Path):
    """Deletes every dataset under the namespace, datasets under other namespaces are untouched."""
    other_key = DatasetKey(namespace=NAMESPACE | {"project_name": "other"}, name=NAME)
    io.write(STEP_KEY, {(0,): (b"s0", {})})
    io.write(
        DatasetKey(namespace=NAMESPACE, name={"component_id": "Model-xyz"}),
        {(): (b"x", {})},
    )
    io.write(other_key, {(): (b"other", {})})

    io.delete_namespace(NAMESPACE)

    assert not (tmp_path / "env=BACKTEST" / "project_name=proj").exists()
    assert io.read(STEP_KEY) == ({}, {})
    assert io.read(other_key) == ({(): b"other"}, {(): {}})


def test_delete_missing_namespace(io: BlobIO):
    """Deleting a namespace with nothing stored is a no-op, so it can be called before every run."""
    io.delete_namespace(NAMESPACE)
    io.write(KEY, {(): (b"x", {})})
    io.delete_namespace(NAMESPACE)
    io.delete_namespace(NAMESPACE)
    assert io.read(KEY) == ({}, {})


def test_delete_namespace_on_a_file_raises(io: BlobIO, tmp_path: Path):
    """A file where the namespace's dir should be isn't a namespace, so it raises instead of being deleted."""
    (tmp_path / "env=BACKTEST").mkdir()
    (tmp_path / "env=BACKTEST" / "project_name=proj").write_bytes(b"not a dir")

    with pytest.raises(RuntimeError, match="is not a directory"):
        io.delete_namespace(NAMESPACE)
    assert (tmp_path / "env=BACKTEST" / "project_name=proj").is_file()
