from pathlib import Path
import re

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
        
        # Keep track of potential delta table directories
        delta_dirs = set()
        
        # First pass: Find _delta_log folders to identify delta tables
        if path.exists():
            for delta_log_dir in path.glob("**/_delta_log"):
                # The parent directory of _delta_log is a delta table
                delta_dirs.add(str(delta_log_dir.parent))
        
        # Scan the directory for files
        if path.exists():
            extensions = ['.parquet', '.csv', '.json', '.arrow', '.delta']
            
            for file_path in path.glob("**/*"):
                if file_path.is_file() and file_path.suffix in extensions:
                    # Try to extract environment info from path
                    cls._extract_env_from_path(file_path, storage_info)
                    
                    # Try parsing with different parsers in order of specificity
                    product_info = None
                    
                    if MarketDataParser.can_parse(file_path):
                        product_info = MarketDataParser.parse(file_path, storage_info)
                    elif NewsDataParser.can_parse(file_path):
                        product_info = NewsDataParser.parse(file_path, storage_info)
                    elif GenericParser.can_parse(file_path):
                        product_info = GenericParser.parse(file_path, storage_info)
                    
                    # Check if file is part of a delta table
                    if product_info:
                        file_dir = str(file_path.parent)
                        # Check if this file is in a directory that's a delta table
                        for delta_dir in delta_dirs:
                            if file_dir.startswith(delta_dir):
                                # Mark this product as a delta table
                                cls._mark_product_as_delta(file_path, product_info.name, storage_info)
        
        return storage_info
    
    @classmethod
    def _mark_product_as_delta(cls, file_path, product_name, storage_info):
        """Mark a product as being a Delta table."""
        # Find the product in the storage_info structure
        for layer in storage_info.layers.values():
            for domain in layer.domains.values():
                for source in domain.sources.values():
                    if product_name in source.products:
                        source.products[product_name].is_delta = True
                        return
    
    @classmethod
    def _extract_env_from_path(cls, file_path, storage_info):
        """Extract environment information from the file path."""
        # Check for env=X pattern in path parts
        path_str = str(file_path)
        env_match = re.search(r'env=(\w+)', path_str)
        if env_match:
            storage_info.env = env_match.group(1).upper()
        # Default fallback (already set in StorageInfo constructor) 