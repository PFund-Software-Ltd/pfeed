from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from pfeed.cli.commands.data.models import get_total_files, get_total_size, format_size

console = Console()

def display_summary(storage_infos, show_stats=True, limit=10):
    """Display summary view of data."""
    console.print()
    
    for storage_info in storage_infos:
        if not storage_info.layers:
            continue  # Skip empty storages
            
        console.print(f"[bold cyan]{storage_info.name}[/bold cyan]")
        console.print(f"  📂 [dim]{storage_info.path}[/dim]")
        console.print(f"  🔹 [bold magenta]env={storage_info.env}[/bold magenta]")
        
        for layer_name, layer_info in storage_info.layers.items():
            console.print(f"  ├── [bold green]{layer_name}[/bold green]")
            
            for domain_name, domain_info in layer_info.domains.items():
                console.print(f"  │   ├── [bold blue]{domain_name}[/bold blue]")
                
                for source_name, source_info in domain_info.sources.items():
                    total_files = get_total_files(source_info.products)
                    total_size = get_total_size(source_info.products)
                    
                    stats_str = f"({total_files} files, {format_size(total_size)})" if show_stats else ""
                    console.print(f"  │   │   ├── [bold]{source_name}[/bold] {stats_str}")
                    
                    # Group products for display
                    products_sorted = sorted(
                        source_info.products.values(),
                        key=lambda p: p.file_count, 
                        reverse=True
                    )
                    
                    # Show only top products if too many
                    display_products = products_sorted[:limit]
                    more_count = len(products_sorted) - limit if len(products_sorted) > limit else 0
                    
                    for i, product in enumerate(display_products):
                        is_last = i == len(display_products) - 1 and more_count == 0
                        prefix = "  │   │   │   └──" if is_last else "  │   │   │   ├──"
                        
                        # Add delta indicator if it's a delta table
                        delta_indicator = "[bold blue]Δ[/bold blue] " if product.is_delta else ""
                        
                        stats_str = f"({product.file_count} files, {format_size(product.size_bytes)})" if show_stats else ""
                        console.print(f"{prefix} {delta_indicator}[bold]{product.name}[/bold] {stats_str}")
                        
                        # Show resolutions
                        resolutions = sorted(product.resolutions)
                        if resolutions:
                            is_last_detail = not product.start_date
                            res_prefix = "  │   │   │       └──" if is_last_detail else "  │   │   │       ├──"
                            console.print(f"{res_prefix} Resolutions: {', '.join(resolutions)}")
                        
                        # Show date range if available
                        if product.start_date and product.end_date:
                            console.print(f"  │   │   │       └── Dates: {product.start_date} to {product.end_date}")
                    
                    # Show ellipsis if more products exist
                    if more_count > 0:
                        console.print(f"  │   │   │   └── ... {more_count} more products")
        
        console.print()  # Add spacing between storage types

def display_table(storage_infos, show_stats=True, limit=10):
    """Display tabular view of data."""
    # Create main table
    table = Table(title="Data Inventory")
    table.add_column("Storage", style="cyan")
    table.add_column("Environment", style="magenta")
    table.add_column("Layer", style="green")
    table.add_column("Domain", style="blue")
    table.add_column("Source")
    table.add_column("Product")
    table.add_column("Format")
    table.add_column("Resolutions")
    table.add_column("Files", justify="right")
    if show_stats:
        table.add_column("Size", justify="right")
    table.add_column("Date Range")
    
    # Add rows to table
    for storage_info in storage_infos:
        if not storage_info.layers:
            continue  # Skip empty storages
            
        for layer_name, layer_info in storage_info.layers.items():
            for domain_name, domain_info in layer_info.domains.items():
                for source_name, source_info in domain_info.sources.items():
                    # Group products for display
                    products_sorted = sorted(
                        source_info.products.values(),
                        key=lambda p: p.file_count, 
                        reverse=True
                    )
                    
                    # Show only top products if too many
                    display_products = products_sorted[:limit]
                    
                    for product in display_products:
                        resolutions = sorted(product.resolutions)
                        resolution_str = ", ".join(resolutions) if resolutions else "N/A"
                        
                        date_range = f"{product.start_date} to {product.end_date}" if product.start_date and product.end_date else "N/A"
                        
                        # Format for display
                        format_str = "Delta" if product.is_delta else "Parquet"
                        
                        # Add row
                        row = [
                            storage_info.name,
                            storage_info.env,
                            layer_name,
                            domain_name,
                            source_name,
                            product.name,
                            format_str,
                            resolution_str,
                            str(product.file_count)
                        ]
                        if show_stats:
                            row.append(format_size(product.size_bytes))
                        row.append(date_range)
                        table.add_row(*row)
                    
                    # Add ellipsis row if more products exist
                    more_count = len(products_sorted) - limit
                    if more_count > 0:
                        ellipsis_row = [
                            storage_info.name,
                            storage_info.env,
                            layer_name,
                            domain_name,
                            source_name,
                            f"... {more_count} more products",
                            "",
                            "",
                            "",
                            "",
                            ""
                        ]
                        if not show_stats:
                            ellipsis_row.pop()  # Remove size column
                        table.add_row(*ellipsis_row)
    
    console.print(table)

def display_tree(storage_infos, show_stats=True, limit=10):
    """Display hierarchical tree view of data."""
    tree = Tree("📍 Data Inventory")
    
    for storage_info in storage_infos:
        if not storage_info.layers:
            continue  # Skip empty storages
            
        # Calculate total files and size for this storage
        total_files = sum(
            get_total_files(source_info.products)
            for layer_info in storage_info.layers.values()
            for domain_info in layer_info.domains.values()
            for source_info in domain_info.sources.values()
        )
        
        total_size = sum(
            get_total_size(source_info.products)
            for layer_info in storage_info.layers.values()
            for domain_info in layer_info.domains.values()
            for source_info in domain_info.sources.values()
        )
        
        stats_str = f"({total_files} files, {format_size(total_size)})" if show_stats else ""
        storage_node = tree.add(f"[bold cyan]{storage_info.name}[/bold cyan] {stats_str}")
        storage_node.add(f"[dim]{storage_info.path}[/dim]")
        storage_node.add(f"[bold magenta]env={storage_info.env}[/bold magenta]")
        
        for layer_name, layer_info in storage_info.layers.items():
            layer_node = storage_node.add(f"[bold green]{layer_name}[/bold green]")
            
            for domain_name, domain_info in layer_info.domains.items():
                domain_node = layer_node.add(f"[bold blue]{domain_name}[/bold blue]")
                
                for source_name, source_info in domain_info.sources.items():
                    source_files = get_total_files(source_info.products)
                    source_size = get_total_size(source_info.products)
                    
                    stats_str = f"({source_files} files, {format_size(source_size)})" if show_stats else ""
                    source_node = domain_node.add(f"[bold]{source_name}[/bold] {stats_str}")
                    
                    # Group products for display
                    products_sorted = sorted(
                        source_info.products.values(),
                        key=lambda p: p.file_count, 
                        reverse=True
                    )
                    
                    # Show only top products if too many
                    display_products = products_sorted[:limit]
                    more_count = len(products_sorted) - limit if len(products_sorted) > limit else 0
                    
                    for product in display_products:
                        stats_str = f"({product.file_count} files, {format_size(product.size_bytes)})" if show_stats else ""
                        
                        # Add delta indicator if it's a delta table
                        delta_indicator = "[bold blue]Δ[/bold blue] " if product.is_delta else ""
                        product_node = source_node.add(f"{delta_indicator}[bold]{product.name}[/bold] {stats_str}")
                        
                        # Show resolutions
                        resolutions = sorted(product.resolutions)
                        if resolutions:
                            product_node.add(f"Resolutions: {', '.join(resolutions)}")
                        
                        # Show date range if available
                        if product.start_date and product.end_date:
                            product_node.add(f"Dates: {product.start_date} to {product.end_date}")
                    
                    # Show ellipsis if more products exist
                    if more_count > 0:
                        source_node.add(f"... {more_count} more products")
    
    console.print(tree)

def display_json(storage_infos):
    """Display data as JSON."""
    import json
    
    # Convert to JSON serializable structure
    json_data = []
    
    for storage_info in storage_infos:
        storage_dict = {
            "name": storage_info.name,
            "path": storage_info.path,
            "env": storage_info.env,
            "layers": []
        }
        
        for layer_name, layer_info in storage_info.layers.items():
            layer_dict = {
                "name": layer_name,
                "domains": []
            }
            
            for domain_name, domain_info in layer_info.domains.items():
                domain_dict = {
                    "name": domain_name,
                    "sources": []
                }
                
                for source_name, source_info in domain_info.sources.items():
                    source_dict = {
                        "name": source_name,
                        "products": []
                    }
                    
                    for product_name, product_info in source_info.products.items():
                        product_dict = {
                            "name": product_name,
                            "is_delta": product_info.is_delta,
                            "resolutions": list(product_info.resolutions),
                            "file_count": product_info.file_count,
                            "size_bytes": product_info.size_bytes,
                            "start_date": product_info.start_date,
                            "end_date": product_info.end_date
                        }
                        source_dict["products"].append(product_dict)
                    
                    domain_dict["sources"].append(source_dict)
                
                layer_dict["domains"].append(domain_dict)
            
            storage_dict["layers"].append(layer_dict)
        
        json_data.append(storage_dict)
    
    # Print JSON
    console.print(json.dumps(json_data, indent=2)) 