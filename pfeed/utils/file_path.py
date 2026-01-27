# VIBE-CODED
from __future__ import annotations
from typing import Union, TYPE_CHECKING
if TYPE_CHECKING:
    from cloudpathlib import CloudPath
    from pfeed.utils.hf_path import HfPath

from pathlib import Path
from urllib.parse import urlparse


class FilePath:
    """
    Unified path class that works with both local paths and cloud storage paths.

    Automatically detects whether the path is local or cloud-based and delegates
    to the appropriate backend (pathlib.Path, CloudPath, or HfPath).

    Examples:
        >>> FilePath("/local/path/file.txt")  # Uses pathlib.Path
        >>> FilePath("s3://bucket/file.txt")   # Uses CloudPath
        >>> FilePath("gs://bucket/file.csv")   # Uses CloudPath
        >>> FilePath("hf://datasets/org/repo/file.parquet")  # Uses HfPath
    """
    
    # Cloud schemes supported by cloudpathlib
    _CLOUDPATH_SCHEMES = frozenset({'s3', 'gs', 'az', 'file'})
    # HuggingFace scheme
    _HF_SCHEME = 'hf'

    def __init__(self, path: Union[str, Path, CloudPath, HfPath, FilePath], **kwargs):
        """Initialize with automatic backend detection.
        
        Args:
            path: Path string or path object
            **kwargs: Additional arguments passed to backend (e.g., token for HfPath)
        """
        from cloudpathlib import CloudPath
        from pfeed.utils.hf_path import HfPath
        
        if isinstance(path, FilePath):
            self._path = path._path
        elif isinstance(path, (Path, CloudPath, HfPath)):
            self._path = path
        else:
            path_str = str(path)
            scheme = self._get_scheme(path_str)
            
            if scheme == self._HF_SCHEME:
                self._path = HfPath(path_str, **kwargs)
            elif scheme in self._CLOUDPATH_SCHEMES:
                self._path = CloudPath(path_str)
            else:
                self._path = Path(path_str)
    
    @staticmethod
    def _get_scheme(path_str: str) -> str | None:
        """Extract scheme from path string."""
        parsed = urlparse(path_str)
        return parsed.scheme if parsed.scheme else None

    @staticmethod
    def _is_cloud_path(path_str: str) -> bool:
        """Check if path string is a cloud path (cloudpathlib only, not HuggingFace)."""
        scheme = FilePath._get_scheme(path_str)
        return scheme in FilePath._CLOUDPATH_SCHEMES
    
    @staticmethod
    def _is_hf_path(path_str: str) -> bool:
        """Check if path string is a HuggingFace path."""
        scheme = FilePath._get_scheme(path_str)
        return scheme == FilePath._HF_SCHEME
    
    @staticmethod
    def _is_remote_path(path_str: str) -> bool:
        """Check if path string is a remote path (cloud or HuggingFace)."""
        return FilePath._is_cloud_path(path_str) or FilePath._is_hf_path(path_str)

    @property
    def is_cloud(self) -> bool:
        """Check if this is a cloud path (cloudpathlib: s3, gs, az)."""
        from cloudpathlib import CloudPath
        return isinstance(self._path, CloudPath)
    
    @property
    def is_hf(self) -> bool:
        """Check if this is a HuggingFace path."""
        from pfeed.utils.hf_path import HfPath
        return isinstance(self._path, HfPath)
    
    @property
    def is_remote(self) -> bool:
        """Check if this is a remote path (cloud or HuggingFace)."""
        return self.is_cloud or self.is_hf

    @property
    def schemeless(self) -> str:
        """
        Returns the path without the URI scheme.

        For cloud paths: 's3://bucket/path/file.parquet' -> 'bucket/path/file.parquet'
        For HF paths: 'hf://datasets/org/repo/file.parquet' -> 'datasets/org/repo/file.parquet'
        For local paths: '/local/path/file.parquet' -> '/local/path/file.parquet'
        """
        if self.is_cloud or self.is_hf:
            parsed = urlparse(str(self._path))
            return f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path
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
        from pfeed.utils.hf_path import HfPath
        state = {
            '_path_str': str(self._path),
            '_is_cloud': self.is_cloud,
            '_is_hf': self.is_hf,
        }
        if isinstance(self._path, HfPath):
            state['_token'] = self._path._token
        return state

    def __setstate__(self, state):
        """Support pickle deserialization (used by Ray)."""
        from cloudpathlib import CloudPath
        from pfeed.utils.hf_path import HfPath
        
        if state.get('_is_hf'):
            self._path = HfPath(state['_path_str'], token=state.get('_token'))
        elif state.get('_is_cloud'):
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
