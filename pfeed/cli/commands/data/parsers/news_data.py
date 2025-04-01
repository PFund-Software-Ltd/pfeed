from pathlib import Path

from pfeed.cli.commands.data.models import StorageInfo
from pfeed.cli.commands.data.parsers.base import BaseParser

class NewsDataParser(BaseParser):
    """Parser for news data paths."""
    
    @classmethod
    def can_parse(cls, path: Path) -> bool:
        """Check if this is a news data path."""
        parts = path.parts
        
        # Check for the presence of data_layer and data_domain
        layer = cls.extract_key_value_from_path(parts, "data_layer")
        domain = cls.extract_key_value_from_path(parts, "data_domain")
        
        if not layer or not domain:
            return False
            
        # Check if this is news_data domain
        return domain == "news_data"
    
    @classmethod
    def parse(cls, path: Path, storage_info: StorageInfo) -> bool:
        """
        Parse news data path and extract information.
        
        Expected path structure:
        data_layer=cleaned/data_domain=news_data/env=BACKTEST/data_source=SOURCE/data_origin=ORIGIN/product_type=TYPE/product=NAME/year=YYYY/month=MM/day=DD/*.parquet
        """
        try:
            parts = path.parts
            
            # Extract key information
            layer_name = cls.extract_key_value_from_path(parts, "data_layer")
            if not layer_name:
                return False
                
            layer_name = layer_name.upper()
            
            domain_name = cls.extract_key_value_from_path(parts, "data_domain")
            if not domain_name or domain_name != "news_data":
                return False
                
            # Extract source and product
            source_name = cls.extract_key_value_from_path(parts, "data_source")
            product_name = cls.extract_key_value_from_path(parts, "product")
            
            if not source_name or not product_name:
                return False
                
            # Extract date information
            year = cls.extract_key_value_from_path(parts, "year")
            month = cls.extract_key_value_from_path(parts, "month")
            day = cls.extract_key_value_from_path(parts, "day")
            
            date_str = f"{year}-{month}-{day}" if year and month and day else None
            
            # Update the storage information hierarchy
            layer_info = storage_info.get_or_create_layer(layer_name)
            domain_info = layer_info.get_or_create_domain(domain_name)
            source_info = domain_info.get_or_create_source(source_name)
            product_info = source_info.get_or_create_product(product_name)
            
            # Update product information (news doesn't have resolutions)
            product_info.file_count += 1
            
            if path.is_file():
                product_info.size_bytes += path.stat().st_size
            
            if date_str:
                product_info.update_dates(date_str)
                
            return True
        except Exception:
            return False 