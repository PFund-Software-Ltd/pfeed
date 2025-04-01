from pathlib import Path
import re

from pfeed.cli.commands.data.models import StorageInfo
from pfeed.cli.commands.data.parsers.base import BaseParser

class MarketDataParser(BaseParser):
    """Parser for market data paths."""
    
    @classmethod
    def can_parse(cls, path: Path) -> bool:
        """Check if this is a market data path."""
        parts = path.parts
        
        # Check for the presence of data_layer
        layer = cls.extract_key_value_from_path(parts, "data_layer")
        if not layer:
            return False
            
        # Check for market_data domain
        domain = cls.extract_key_value_from_path(parts, "data_domain")
        if not domain:
            return False
            
        # Check if this is market_data domain
        return domain == "market_data"
    
    @classmethod
    def parse(cls, path: Path, storage_info: StorageInfo) -> bool:
        """
        Parse market data path and extract information.
        
        Expected path structures:
        - CLEANED/RAW: data_layer=cleaned/data_domain=market_data/env=BACKTEST/data_source=SOURCE/data_origin=ORIGIN/product_type=TYPE/product=NAME/resolution=RES/year=YYYY/month=MM/day=DD/*.parquet
        - CURATED: data_layer=curated/data_domain=market_data/env=BACKTEST/data_source=SOURCE/data_origin=ORIGIN/PRODUCT_NAME.parquet
        """
        try:
            parts = path.parts
            
            # Extract key information
            layer_name = cls.extract_key_value_from_path(parts, "data_layer")
            if not layer_name:
                return False
                
            layer_name = layer_name.upper()
            
            domain_name = cls.extract_key_value_from_path(parts, "data_domain")
            if not domain_name or domain_name != "market_data":
                return False
            
            # Create layer and domain info
            layer_info = storage_info.get_or_create_layer(layer_name)
            domain_info = layer_info.get_or_create_domain(domain_name)
            
            # Extract source (common to both path structures)
            source_name = cls.extract_key_value_from_path(parts, "data_source")
            if not source_name:
                return False
                
            source_info = domain_info.get_or_create_source(source_name)
            
            # Different parsing logic based on data layer
            if layer_name == "CURATED":
                # For CURATED data, extract product from filename
                if path.is_file():
                    # Extract product name from the filename
                    product_name = path.stem
                    
                    # If product_name contains underscores, it's likely a structured name like "BTC_USDT_PERP"
                    # Parse out any valuable information if needed
                    
                    product_info = source_info.get_or_create_product(product_name)
                    
                    # CURATED might not have resolutions in path
                    resolution = cls.extract_key_value_from_path(parts, "resolution")
                    if resolution:
                        product_info.add_resolution(resolution)
                    else:
                        # Try to infer resolution from filename
                        res_match = re.search(r'_(\d+[smhd])_', product_name)
                        if res_match:
                            product_info.add_resolution(res_match.group(1))
                        
                    # Update stats
                    product_info.file_count += 1
                    product_info.size_bytes += path.stat().st_size
                        
                    return True
                return False
            else:
                # For CLEANED/RAW, use standard path structure
                product_name = cls.extract_key_value_from_path(parts, "product")
                resolution = cls.extract_key_value_from_path(parts, "resolution")
                
                if not product_name or not resolution:
                    return False
                    
                # Extract date information
                year = cls.extract_key_value_from_path(parts, "year")
                month = cls.extract_key_value_from_path(parts, "month")
                day = cls.extract_key_value_from_path(parts, "day")
                
                date_str = f"{year}-{month}-{day}" if year and month and day else None
                
                # Update product information
                product_info = source_info.get_or_create_product(product_name)
                product_info.add_resolution(resolution)
                product_info.file_count += 1
                
                if path.is_file():
                    product_info.size_bytes += path.stat().st_size
                
                if date_str:
                    product_info.update_dates(date_str)
                    
                return True
        except Exception as e:
            # Uncomment for debugging
            # import logging
            # logging.getLogger('pfeed').debug(f"Error parsing market data path: {path}, error: {e}")
            return False 