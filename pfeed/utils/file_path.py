from __future__ import annotations
from typing import Union, TYPE_CHECKING
if TYPE_CHECKING:
    from cloudpathlib import CloudPath

from pathlib import Path
from urllib.parse import urlparse


class FilePath:
    """
    Unified path class that works with both local paths and cloud storage paths.

    Automatically detects whether the path is local or cloud-based and delegates
    to the appropriate backend (pathlib.Path or CloudPath).

    Examples:
        >>> FilePath("/local/path/file.txt")  # Uses pathlib.Path
        >>> FilePath("s3://bucket/file.txt")   # Uses CloudPath
        >>> FilePath("gs://bucket/file.csv")   # Uses CloudPath
    """

    def __init__(self, path: Union[str, Path, CloudPath, FilePath]):
        """Initialize with automatic backend detection."""
        from cloudpathlib import CloudPath
        
        if isinstance(path, FilePath):
            self._path = path._path
        elif isinstance(path, (Path, CloudPath)):
            self._path = path
        else:
            path_str = str(path)
            # Check if it's a cloud path (has scheme like s3://, gs://, az://)
            if self._is_cloud_path(path_str):
                self._path = CloudPath(path_str)
            else:
                self._path = Path(path_str)

    @staticmethod
    def _is_cloud_path(path_str: str) -> bool:
        """Check if path string is a cloud path."""
        parsed = urlparse(path_str)
        # Cloud paths have schemes like s3, gs, az
        return parsed.scheme in ('s3', 'gs', 'az', 'file')

    @property
    def is_cloud(self) -> bool:
        """Check if this is a cloud path."""
        from cloudpathlib import CloudPath
        return isinstance(self._path, CloudPath)

    @property
    def schemeless(self) -> str:
        """
        Returns the path without the URI scheme.

        For cloud paths: 's3://bucket/path/file.parquet' -> 'bucket/path/file.parquet'
        For local paths: '/local/path/file.parquet' -> '/local/path/file.parquet'
        """
        if self.is_cloud:
            parsed = urlparse(str(self._path))
            return f"{parsed.netloc}{parsed.path}"
        return str(self._path)

    # Delegate all attribute access to the underlying path object
    def __getattr__(self, name):
        # Guard against infinite recursion during pickle deserialization
        # When unpickling, _path doesn't exist yet, causing __getattr__ to be called for it
        if name == '_path':
            raise AttributeError(name)
        return getattr(self._path, name)

    def __getstate__(self):
        """Support pickle serialization (used by Ray)."""
        return {'_path_str': str(self._path), '_is_cloud': self.is_cloud}

    def __setstate__(self, state):
        """Support pickle deserialization (used by Ray)."""
        from cloudpathlib import CloudPath
        if state['_is_cloud']:
            self._path = CloudPath(state['_path_str'])
        else:
            self._path = Path(state['_path_str'])

    def __str__(self):
        return str(self._path)

    def __repr__(self):
        return f"FilePath({str(self._path)!r})"

    def __truediv__(self, other):
        """Support path / 'subpath' syntax."""
        return FilePath(self._path / other)

    def __rtruediv__(self, other):
        """Support 'base' / path syntax."""
        return FilePath(other) / self

    def __eq__(self, other):
        if isinstance(other, FilePath):
            return self._path == other._path
        return self._path == other

    def __hash__(self):
        return hash(self._path)
