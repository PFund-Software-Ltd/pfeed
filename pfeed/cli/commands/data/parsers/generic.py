from pathlib import Path

from pfeed.cli.commands.data.models import StorageInfo
from pfeed.cli.commands.data.parsers.base import BaseParser

class GenericParser(BaseParser):
    """Parser for generic data paths that don't match specific formats."""
    
    @classmethod
    def can_parse(cls, path: Path) -> bool:
        """
        Generic parser can parse any path that has at least data_layer and data_domain.
        This should be used as a last resort after specific parsers have failed.
        """
        parts = path.parts
        
        # Check for the presence of data_layer and data_domain
        layer = cls.extract_key_value_from_path(parts, "data_layer")
        domain = cls.extract_key_value_from_path(parts, "data_domain")
        
        return layer is not None and domain is not None
    
    @classmethod
    def parse(cls, path: Path, storage_info: StorageInfo) -> bool:
        """
        Parse a generic data path and extract as much information as possible.
        """
        try:
            parts = path.parts
            
            # Extract key information
            layer_name = cls.extract_key_value_from_path(parts, "data_layer")
            if not layer_name:
                return False
                
            layer_name = layer_name.upper()
            
            domain_name = cls.extract_key_value_from_path(parts, "data_domain")
            if not domain_name:
                return False
                
            # Try to find source name (data_source or env)
            source_name = cls.extract_key_value_from_path(parts, "data_source")
            if not source_name:
                source_name = cls.extract_key_value_from_path(parts, "env")
            if not source_name:
                source_name = "unknown"
            
            # Try to find product name
            product_name = cls.extract_key_value_from_path(parts, "product")
            if not product_name:
                # Use filename as product name 
                filename = path.stem if path.is_file() else ""
                
                # Try to parse product from filename
                if filename:
                    parts = filename.split('_')
                    # Check if last part looks like a date (YYYY-MM-DD)
                    if len(parts) >= 2 and parts[-1].count('-') == 2:
                        # Assume format like BTC_USDT_PERP_2023-01-01
                        product_name = '_'.join(parts[:-1])
                    else:
                        product_name = filename
                else:
                    product_name = "unknown_product"
                    
            # Extract resolution if possible
            resolution = cls.extract_key_value_from_path(parts, "resolution")
            resolutions = set()
            
            if resolution:
                resolutions.add(resolution)
            else:
                # Try to extract from filename
                filename = path.stem if path.is_file() else ""
                if filename:
                    parts = filename.split('_')
                    # Look for parts that match resolution formats (1m, 15m, 1h, 1d, etc.)
                    for part in parts:
                        if part.startswith(('1', '5', '15', '30', '60')) and len(part) <= 3:
                            if part[-1] in ['m', 'h', 'd', 't', 's']:
                                resolutions.add(part)
                                break
            
            # Extract date information
            year = cls.extract_key_value_from_path(parts, "year")
            month = cls.extract_key_value_from_path(parts, "month")
            day = cls.extract_key_value_from_path(parts, "day")
            
            date_str = None
            if year and month and day:
                date_str = f"{year}-{month}-{day}"
            else:
                # Try to find date in filename
                filename = path.stem if path.is_file() else ""
                if filename:
                    parts = filename.split('_')
                    for part in parts:
                        if part.count('-') == 2:
                            try:
                                # Validate it's a date (YYYY-MM-DD)
                                y, m, d = part.split('-')
                                if len(y) == 4 and len(m) == 2 and len(d) == 2:
                                    date_str = part
                                    break
                            except:
                                pass
            
            # Update the storage information hierarchy
            layer_info = storage_info.get_or_create_layer(layer_name)
            domain_info = layer_info.get_or_create_domain(domain_name)
            source_info = domain_info.get_or_create_source(source_name)
            product_info = source_info.get_or_create_product(product_name)
            
            # Add resolutions
            for res in resolutions:
                product_info.add_resolution(res)
            
            # Update stats
            product_info.file_count += 1
            if path.is_file():
                product_info.size_bytes += path.stat().st_size
            
            if date_str:
                product_info.update_dates(date_str)
                
            return True
        except Exception:
            return False 