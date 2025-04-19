from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from pfeed.cli.commands.data.models import StorageInfo, ProductInfo

class BaseParser(ABC):
    """Base class for data path parsers."""
    
    @classmethod
    @abstractmethod
    def can_parse(cls, path: Path) -> bool:
        """
        Check if this parser can handle the given path.
        
        Args:
            path: Path to check
            
        Returns:
            True if this parser can handle the path, False otherwise
        """
        pass
    
    @classmethod
    @abstractmethod
    def parse(cls, path: Path, storage_info: StorageInfo) -> Optional[ProductInfo]:
        """
        Parse the given path and update storage_info accordingly.
        
        Args:
            path: Path to parse
            storage_info: StorageInfo object to update
            
        Returns:
            ProductInfo object if parsing was successful, None otherwise
        """
        pass
    
    @staticmethod
    def extract_key_value_from_path(path_parts: list, key: str) -> Optional[str]:
        """
        Extract a value from a path segment in the format "key=value".
        
        Args:
            path_parts: List of path segments
            key: Key to search for
            
        Returns:
            The extracted value, or None if not found
        """
        for part in path_parts:
            if part.startswith(f"{key}="):
                return part.replace(f"{key}=", "")
        return None 