"""Helpers shared by the IOs storing each partition as files in a hive layout (ParquetIO, BlobIO)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from pfeed.io.base_io import PartitionValue

import datetime
import os
import sys
import time
from urllib.parse import urlparse

import pyarrow.fs as pafs


def retry_on_windows_lock[**P, T](
    fn: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Retries fn while another process briefly holds the file (Windows only).

    On POSIX, replacing a file (rename) is atomic and never blocks readers. On Windows, opening a file while another
    process replaces it, or replacing a file another process has open, fails with a sharing violation
    (PermissionError, WinError 32/5). Concurrent writers hit this, since every write reads all partitions' footers.
    """
    if sys.platform != "win32":
        return fn(*args, **kwargs)
    delay, deadline = 0.01, time.monotonic() + 5
    while True:
        try:
            return fn(*args, **kwargs)
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 0.2)


def resolve_local_base_path(
    base_path: str | os.PathLike[str] | None, default_dir_name: str
) -> tuple[pafs.FileSystem, str]:
    """Returns (filesystem, base_path) of an IO's root directory.

    Args:
        base_path: root directory of all datasets. Local paths only for now.
            Defaults to <config.data_path>/<default_dir_name>.
    """
    if base_path is None:
        from pfeed.config import get_config

        base_path = get_config().data_path / default_dir_name
    # urlparse only takes str/bytes, so a Path must become a str first
    base_path = os.fspath(base_path)
    scheme = urlparse(base_path).scheme
    # a one-letter scheme is a Windows drive letter (C:\...), not a URI scheme
    if scheme not in ("", "file") and len(scheme) > 1:
        raise NotImplementedError(
            f"only local paths are supported for now, got {base_path!r}"
        )
    return pafs.FileSystem.from_uri(os.path.abspath(base_path.removeprefix("file://")))


def to_hive_dir(column: str, value: PartitionValue) -> str:
    if isinstance(value, datetime.datetime) or not isinstance(
        value, (str, int, datetime.date)
    ):
        raise TypeError(f"unsupported partition value {value!r} for {column!r}")
    value = value.isoformat() if isinstance(value, datetime.date) else str(value)
    if not value or "/" in value:
        raise ValueError(f"invalid partition value {value!r} for {column!r}")
    return f"{column}={value}"
