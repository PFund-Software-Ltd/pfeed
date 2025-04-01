from abc import ABC, abstractmethod

from pfeed.cli.commands.data.models import StorageInfo
from pfeed.enums import DataStorage

class BaseCollector(ABC):
    """Base class for data collectors."""
    
    @classmethod
    @abstractmethod
    def collect(cls, storage_type: DataStorage) -> StorageInfo:
        """
        Collect information about data in a specific storage.
        
        Args:
            storage_type: The type of storage to collect information from
            
        Returns:
            StorageInfo object with collected information
        """
        pass 