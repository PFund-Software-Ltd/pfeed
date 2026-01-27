# VIBE-CODED
from __future__ import annotations
from typing import TYPE_CHECKING
import os
from functools import cached_property

if TYPE_CHECKING:
    from huggingface_hub import HfFileSystem


class HfPath:
    """
    A pathlib-like wrapper for HuggingFace Hub paths.
    
    Provides a Path-like interface for hf:// URIs, compatible with
    the FilePath abstraction used throughout pfeed.
    
    Examples:
        >>> path = HfPath("hf://datasets/pfund-ai/market-data/file.parquet")
        >>> path.name
        'file.parquet'
        >>> path.parent
        HfPath('hf://datasets/pfund-ai/market-data')
    """
    
    _SCHEME = "hf://"
    
    def __init__(self, path: str | HfPath, token: str | None = None):
        """
        Initialize HfPath.
        
        Args:
            path: HuggingFace path, with or without hf:// prefix
            token: HuggingFace API token (defaults to HF_TOKEN env var)
        """
        if isinstance(path, HfPath):
            self._path_str = path._path_str
            self._token = path._token
        else:
            # Normalize path: ensure no scheme prefix in internal storage
            path_str = str(path)
            if path_str.startswith(self._SCHEME):
                path_str = path_str[len(self._SCHEME):]
            # Remove trailing slashes for consistency
            self._path_str = path_str.rstrip("/")
            self._token = token or os.getenv("HF_TOKEN")
    
    @cached_property
    def _fs(self) -> HfFileSystem:
        """Lazily create HfFileSystem instance."""
        from huggingface_hub import HfFileSystem
        return HfFileSystem(token=self._token)
    
    @property
    def _full_path(self) -> str:
        """Full path with hf:// scheme."""
        return f"{self._SCHEME}{self._path_str}"
    
    # === pathlib.Path-like properties ===
    
    @property
    def name(self) -> str:
        """The final component of the path."""
        return self._path_str.rsplit("/", 1)[-1] if "/" in self._path_str else self._path_str
    
    @property
    def stem(self) -> str:
        """The final component without its suffix."""
        name = self.name
        if "." in name:
            return name.rsplit(".", 1)[0]
        return name
    
    @property
    def suffix(self) -> str:
        """The file extension of the final component."""
        name = self.name
        if "." in name:
            return "." + name.rsplit(".", 1)[-1]
        return ""
    
    @property
    def suffixes(self) -> list[str]:
        """A list of the path's file extensions."""
        name = self.name
        if "." not in name:
            return []
        parts = name.split(".")
        return ["." + ext for ext in parts[1:]]
    
    @property
    def parent(self) -> HfPath:
        """The logical parent of the path."""
        if "/" not in self._path_str:
            return HfPath(self._path_str, token=self._token)
        parent_path = self._path_str.rsplit("/", 1)[0]
        return HfPath(parent_path, token=self._token)
    
    @property
    def parents(self) -> tuple[HfPath, ...]:
        """An immutable sequence of the logical ancestors of the path."""
        parents = []
        current = self
        while current._path_str != current.parent._path_str:
            current = current.parent
            parents.append(current)
        return tuple(parents)
    
    @property
    def parts(self) -> tuple[str, ...]:
        """An object providing sequence-like access to the path's components."""
        return tuple(self._path_str.split("/"))
    
    # === Filesystem operations ===
    
    def exists(self) -> bool:
        """Check if this path exists on HuggingFace Hub."""
        return self._fs.exists(self._path_str)
    
    def is_file(self) -> bool:
        """Check if this path is a file."""
        return self._fs.isfile(self._path_str)
    
    def is_dir(self) -> bool:
        """Check if this path is a directory."""
        return self._fs.isdir(self._path_str)
    
    def mkdir(self, parents: bool = False, exist_ok: bool = False):
        """
        Create a directory at this path.
        
        Note: HuggingFace Hub doesn't have true directories - they are
        created implicitly when files are uploaded. This is a no-op
        that maintains API compatibility with pathlib.Path.
        """
        # HuggingFace Hub creates directories implicitly on file upload
        # This is a no-op for API compatibility
        pass
    
    def iterdir(self):
        """Iterate over the directory contents."""
        for item in self._fs.ls(self._path_str, detail=False):
            yield HfPath(item, token=self._token)
    
    def glob(self, pattern: str):
        """Glob the given pattern in this directory."""
        full_pattern = f"{self._path_str}/{pattern}"
        for match in self._fs.glob(full_pattern):
            yield HfPath(match, token=self._token)
    
    def rglob(self, pattern: str):
        """Recursive glob."""
        return self.glob(f"**/{pattern}")
    
    def read_bytes(self) -> bytes:
        """Read the file contents as bytes."""
        with self._fs.open(self._path_str, "rb") as f:
            return f.read()
    
    def read_text(self, encoding: str = "utf-8") -> str:
        """Read the file contents as text."""
        return self.read_bytes().decode(encoding)
    
    def write_bytes(self, data: bytes):
        """Write bytes to the file."""
        with self._fs.open(self._path_str, "wb") as f:
            f.write(data)
    
    def write_text(self, data: str, encoding: str = "utf-8"):
        """Write text to the file."""
        self.write_bytes(data.encode(encoding))
    
    def unlink(self, missing_ok: bool = False):
        """Remove this file."""
        if not missing_ok and not self.exists():
            raise FileNotFoundError(f"No such file: {self._full_path}")
        self._fs.rm(self._path_str)
    
    def rmdir(self):
        """Remove this directory (must be empty)."""
        self._fs.rmdir(self._path_str)
    
    def stat(self):
        """Return file info (similar to os.stat)."""
        return self._fs.info(self._path_str)
    
    # === Path manipulation ===
    
    def joinpath(self, *others) -> HfPath:
        """Combine this path with one or several arguments."""
        parts = [self._path_str] + [str(o) for o in others]
        return HfPath("/".join(parts), token=self._token)
    
    def with_name(self, name: str) -> HfPath:
        """Return a new path with the file name changed."""
        return self.parent / name
    
    def with_stem(self, stem: str) -> HfPath:
        """Return a new path with the stem changed."""
        return self.parent / (stem + self.suffix)
    
    def with_suffix(self, suffix: str) -> HfPath:
        """Return a new path with the file suffix changed."""
        return self.parent / (self.stem + suffix)
    
    # === Dunder methods ===
    
    def __truediv__(self, other) -> HfPath:
        """Support path / 'subpath' syntax."""
        other_str = str(other)
        if other_str.startswith("/"):
            other_str = other_str[1:]
        new_path = f"{self._path_str}/{other_str}"
        return HfPath(new_path, token=self._token)
    
    def __rtruediv__(self, other) -> HfPath:
        """Support 'base' / path syntax."""
        return HfPath(other, token=self._token) / self._path_str
    
    def __str__(self) -> str:
        """Return the full path with scheme."""
        return self._full_path
    
    def __repr__(self) -> str:
        return f"HfPath({self._full_path!r})"
    
    def __fspath__(self) -> str:
        """Support os.fspath()."""
        return self._full_path
    
    def __eq__(self, other) -> bool:
        if isinstance(other, HfPath):
            return self._path_str == other._path_str
        if isinstance(other, str):
            other_normalized = other.removeprefix(self._SCHEME).rstrip("/")
            return self._path_str == other_normalized
        return False
    
    def __hash__(self) -> int:
        return hash(self._path_str)
    
    def __lt__(self, other) -> bool:
        if isinstance(other, HfPath):
            return self._path_str < other._path_str
        return NotImplemented
    
    # === Pickle support (for Ray) ===
    
    def __getstate__(self):
        """Support pickle serialization."""
        return {"_path_str": self._path_str, "_token": self._token}
    
    def __setstate__(self, state):
        """Support pickle deserialization."""
        self._path_str = state["_path_str"]
        self._token = state["_token"]
