from dataclasses import dataclass, field
from typing import Dict, Set, Optional

@dataclass
class ProductInfo:
    """Information about a product and its available data."""
    name: str
    resolutions: Set[str] = field(default_factory=set)
    file_count: int = 0
    size_bytes: int = 0
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    
    def add_resolution(self, resolution: str):
        self.resolutions.add(resolution)
        
    def update_dates(self, date: str):
        if self.start_date is None or date < self.start_date:
            self.start_date = date
        if self.end_date is None or date > self.end_date:
            self.end_date = date

@dataclass
class DataSourceInfo:
    """Information about a data source."""
    name: str
    products: Dict[str, ProductInfo] = field(default_factory=dict)
    
    def get_or_create_product(self, product_name: str) -> ProductInfo:
        if product_name not in self.products:
            self.products[product_name] = ProductInfo(name=product_name)
        return self.products[product_name]

@dataclass
class DataDomainInfo:
    """Information about a data domain."""
    name: str
    sources: Dict[str, DataSourceInfo] = field(default_factory=dict)
    
    def get_or_create_source(self, source_name: str) -> DataSourceInfo:
        if source_name not in self.sources:
            self.sources[source_name] = DataSourceInfo(name=source_name)
        return self.sources[source_name]

@dataclass
class DataLayerInfo:
    """Information about a data layer."""
    name: str
    domains: Dict[str, DataDomainInfo] = field(default_factory=dict)
    
    def get_or_create_domain(self, domain_name: str) -> DataDomainInfo:
        if domain_name not in self.domains:
            self.domains[domain_name] = DataDomainInfo(name=domain_name)
        return self.domains[domain_name]

@dataclass
class StorageInfo:
    """Information about a storage location."""
    name: str
    path: str
    layers: Dict[str, DataLayerInfo] = field(default_factory=dict)
    
    def get_or_create_layer(self, layer_name: str) -> DataLayerInfo:
        if layer_name not in self.layers:
            self.layers[layer_name] = DataLayerInfo(name=layer_name)
        return self.layers[layer_name]

def format_size(size_bytes):
    """Format size in bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def get_total_size(products):
    """Calculate total size across products."""
    return sum(product.size_bytes for product in products.values())

def get_total_files(products):
    """Calculate total file count across products."""
    return sum(product.file_count for product in products.values()) 