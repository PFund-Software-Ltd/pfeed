from pathlib import Path
import re

from pfeed.cli.commands.data.models import StorageInfo
from pfeed.cli.commands.data.parsers.base import BaseParser

class GenericParser(BaseParser):
    """Generic parser for data paths that don't match other specialized parsers."""
    
    @classmethod
    def can_parse(cls, path: Path) -> bool:
        """This parser can handle any path, as it's a fallback."""
        return True
    
    @classmethod
    def parse(cls, path: Path, storage_info: StorageInfo):
        """
        Parse a generic data path using heuristics.
        
        This is a fallback parser that tries to extract information from a path
        using common patterns and heuristics.
        
        Returns:
            ProductInfo object if successful, None otherwise
        """
        try:
            # Check if path is a file
            if not path.is_file():
                return None
                
            # Extract information from path parts
            parts = path.parts
            
            # Try to find layer, domain, source and product information
            # Start with explicit key=value pairs in path
            layer_name = cls.extract_key_value_from_path(parts, "data_layer") or \
                        cls.extract_key_value_from_path(parts, "layer")
                        
            domain_name = cls.extract_key_value_from_path(parts, "data_domain") or \
                         cls.extract_key_value_from_path(parts, "domain")
                         
            source_name = cls.extract_key_value_from_path(parts, "data_source") or \
                         cls.extract_key_value_from_path(parts, "source")
            
            product_name = cls.extract_key_value_from_path(parts, "product")
            
            # If key=value pairs are not found, try to infer from directory structure
            if not layer_name:
                # Try looking for common layer names in parts
                for i, part in enumerate(parts):
                    if part.lower() in ["raw", "cleaned", "curated"]:
                        layer_name = part.upper()
                        # If the next part exists, it might be the domain
                        if not domain_name and i+1 < len(parts):
                            domain_name = parts[i+1]
                        break
            
            if not layer_name:
                # Default to UNKNOWN if not found
                layer_name = "UNKNOWN"
                
            if not domain_name:
                # Try to infer domain from directories
                for part in parts:
                    if part.lower() in ["market_data", "news", "fundamentals", "alternative"]:
                        domain_name = part.lower()
                        break
                        
                # Default to generic if not found
                if not domain_name:
                    domain_name = "generic"
            
            # Infer source if not found
            if not source_name:
                # Look for known source patterns
                for part in parts:
                    if part.upper() in ["YAHOO_FINANCE", "BYBIT", "FINANCIAL_MODELING_PREP", "FMP"]:
                        source_name = part.upper()
                        break
                
                # Try to infer from filename patterns
                if not source_name:
                    filename = path.name.lower()
                    if "yahoo" in filename:
                        source_name = "YAHOO_FINANCE"
                    elif "bybit" in filename:
                        source_name = "BYBIT"
                    elif "binance" in filename:
                        source_name = "BINANCE"
                    elif "fmp" in filename or "financial" in filename:
                        source_name = "FINANCIAL_MODELING_PREP"
                    else:
                        # Use a generic source name based on path
                        source_name = "UNKNOWN"
            
            # Extract product information from filename if not explicitly in path
            if not product_name:
                # Remove extension and date patterns from filename
                name_parts = path.stem.split('_')
                
                # Filter out parts that look like dates
                date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
                name_parts = [p for p in name_parts if not date_pattern.match(p)]
                
                if name_parts:
                    product_name = '_'.join(name_parts)
                else:
                    # Default to filename if can't extract
                    product_name = path.stem
            
            # Create layer and domain info
            layer_info = storage_info.get_or_create_layer(layer_name)
            domain_info = layer_info.get_or_create_domain(domain_name)
            source_info = domain_info.get_or_create_source(source_name)
            product_info = source_info.get_or_create_product(product_name)
            
            # Try to extract resolution
            resolution = cls.extract_key_value_from_path(parts, "resolution")
            if resolution:
                product_info.add_resolution(resolution)
            else:
                # Try to infer resolution from filename
                res_pattern = re.compile(r'(\d+[smhd])')
                res_match = res_pattern.search(path.stem)
                if res_match:
                    product_info.add_resolution(res_match.group(1))
                else:
                    # Default resolution
                    product_info.add_resolution("1d")
            
            # Update stats
            product_info.file_count += 1
            product_info.size_bytes += path.stat().st_size
            
            # Try to extract date from path or filename
            date_str = None
            
            # First check for year/month/day in path
            year = cls.extract_key_value_from_path(parts, "year")
            month = cls.extract_key_value_from_path(parts, "month")
            day = cls.extract_key_value_from_path(parts, "day")
            
            if year and month and day:
                date_str = f"{year}-{month}-{day}"
            else:
                # Try to extract date from filename
                date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')
                date_match = date_pattern.search(path.stem)
                if date_match:
                    date_str = date_match.group(1)
            
            if date_str:
                product_info.update_dates(date_str)
                
            return product_info
        except Exception as e:
            # Uncomment for debugging
            # import logging
            # logging.getLogger('pfeed').debug(f"Error parsing generic path: {path}, error: {e}")
            return None 