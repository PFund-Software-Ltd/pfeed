import click

from pfeed.cli.commands.data.collectors import LocalCollector, MinioCollector, DuckDBCollector
from pfeed.cli.commands.data.formatters import display_summary, display_table, display_tree, display_json
from pfeed.enums import DataStorage, DataLayer
import pfeed as pe

@click.group()
def data():
    """Manage and explore data stored in various storage locations."""
    pass

@data.command()
@click.option('--storage', type=click.Choice([s.name for s in DataStorage], case_sensitive=False), 
              help='Filter by storage type')
@click.option('--layer', type=click.Choice([l.name for l in DataLayer], case_sensitive=False), 
              help='Filter by data layer')
@click.option('--domain', help='Filter by data domain')
@click.option('--source', help='Filter by data source')
@click.option('--product', help='Filter by product name')
@click.option('--resolution', help='Filter by resolution')
@click.option('--format', 'output_format', type=click.Choice(['summary', 'table', 'tree', 'json']), 
              default='summary', help='Output format')
@click.option('--stats/--no-stats', default=True, help='Show statistics (size, count)')
@click.option('--limit', type=int, default=10, help='Limit number of items to display per category')
@click.option('--data-path', type=click.Path(exists=False), help='Override data path')
@click.option('--env-file', 'env_file_path', type=click.Path(exists=True), help='Path to the .env file')
@click.option('--debug', is_flag=True, help='Enable debug mode')
def list(storage, layer, domain, source, product, resolution, output_format, stats, limit, data_path, env_file_path, debug):
    """List data across all storages."""
    # Configure pfeed if needed
    if data_path or env_file_path or debug:
        pe.configure(data_path=data_path, env_file_path=env_file_path, debug=debug)
    
    # Determine which storage types to check
    if storage:
        storage_types = [DataStorage[storage.upper()]]
    else:
        # Get all storage types
        storage_types = [DataStorage[s] for s in DataStorage.__members__]
    
    # Collect information from each storage using the appropriate collector
    storage_infos = []
    
    for storage_type in storage_types:
        storage_info = None
        
        # Use the appropriate collector for each storage type
        if storage_type in [DataStorage.LOCAL, DataStorage.CACHE]:
            storage_info = LocalCollector.collect(storage_type)
        elif storage_type == DataStorage.MINIO:
            storage_info = MinioCollector.collect(storage_type)
        elif storage_type == DataStorage.DUCKDB:
            storage_info = DuckDBCollector.collect(storage_type)
        
        if storage_info and storage_info.layers:
            # Apply filters
            if layer:
                layer_name = layer.upper()
                storage_info.layers = {name: info for name, info in storage_info.layers.items() 
                                     if name == layer_name}
            
            if domain:
                for layer_info in storage_info.layers.values():
                    layer_info.domains = {name: info for name, info in layer_info.domains.items()
                                        if name == domain}
            
            if source:
                for layer_info in storage_info.layers.values():
                    for domain_info in layer_info.domains.values():
                        domain_info.sources = {name: info for name, info in domain_info.sources.items()
                                            if name == source}
            
            if product or resolution:
                for layer_info in storage_info.layers.values():
                    for domain_info in layer_info.domains.values():
                        for source_info in domain_info.sources.values():
                            filtered_products = {}
                            
                            for name, prod_info in source_info.products.items():
                                if product and product.lower() not in name.lower():
                                    continue
                                    
                                if resolution and not any(resolution.lower() in res.lower() for res in prod_info.resolutions):
                                    continue
                                    
                                filtered_products[name] = prod_info
                                
                            source_info.products = filtered_products
            
            # Only add storage if it has data after filtering
            has_data = any(
                any(
                    any(
                        source_info.products
                        for source_info in domain_info.sources.values()
                    )
                    for domain_info in layer_info.domains.values()
                )
                for layer_info in storage_info.layers.values()
            )
            
            if has_data:
                storage_infos.append(storage_info)
    
    # Display data based on format
    if output_format == 'summary':
        display_summary(storage_infos, stats, limit)
    elif output_format == 'table':
        display_table(storage_infos, stats, limit)
    elif output_format == 'tree':
        display_tree(storage_infos, stats, limit)
    elif output_format == 'json':
        display_json(storage_infos) 