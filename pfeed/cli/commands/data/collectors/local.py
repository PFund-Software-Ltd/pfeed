from pathlib import Path

from pfeed.cli.commands.data.collectors.base import BaseCollector
from pfeed.cli.commands.data.models import StorageInfo
from pfeed.cli.commands.data.parsers import MarketDataParser, NewsDataParser, GenericParser
from pfeed.config import get_config
from pfeed.enums import DataStorage

class LocalCollector(BaseCollector):
    """Collector for local file system data (LOCAL and CACHE)."""
    
    @classmethod
    def collect(cls, storage_type: DataStorage) -> StorageInfo:
        """
        Collect information about data in the local file system.
        
        Args:
            storage_type: LOCAL or CACHE
            
        Returns:
            StorageInfo object with collected information
        """
        config = get_config()
        
        # Determine the appropriate path based on storage type
        if storage_type == DataStorage.LOCAL:
            path = Path(config.data_path)
            storage_info = StorageInfo(name=storage_type.name, path=str(path))
        elif storage_type == DataStorage.CACHE:
            path = Path(config.cache_path)
            storage_info = StorageInfo(name=storage_type.name, path=str(path))
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        # Scan the directory for files
        if path.exists():
            extensions = ['.parquet', '.csv', '.json', '.arrow', '.delta']
            
            for file_path in path.glob("**/*"):
                if file_path.is_file() and file_path.suffix in extensions:
                    # Try parsing with different parsers in order of specificity
                    if MarketDataParser.can_parse(file_path):
                        MarketDataParser.parse(file_path, storage_info)
                    elif NewsDataParser.can_parse(file_path):
                        NewsDataParser.parse(file_path, storage_info)
                    elif GenericParser.can_parse(file_path):
                        GenericParser.parse(file_path, storage_info)
        
        return storage_info 