from pathlib import Path
import re

from rich.console import Console

from pfeed.cli.commands.data.collectors.base import BaseCollector
from pfeed.cli.commands.data.models import StorageInfo
from pfeed.config import get_config
from pfeed.enums import DataStorage

console = Console()

class DuckDBCollector(BaseCollector):
    """Collector for DuckDB data."""
    
    @classmethod
    def collect(cls, storage_type: DataStorage) -> StorageInfo:
        """
        Collect information about data in DuckDB.
        
        Args:
            storage_type: Must be DUCKDB
            
        Returns:
            StorageInfo object with collected information
        """
        if storage_type != DataStorage.DUCKDB:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        config = get_config()
        duckdb_path = Path(config.data_path) / "duckdb"
        
        storage_info = StorageInfo(name=storage_type.name, path=str(duckdb_path))
        
        # Check if duckdb path exists
        if not duckdb_path.exists():
            return storage_info
        
        try:
            import duckdb
            
            # Scan for DuckDB files in the directory
            db_files = list(duckdb_path.glob("*.db"))
            
            for db_file in db_files:
                # Try to determine layer and domain from filename
                # Typically named like "cleaned_market_data.db"
                parts = db_file.stem.split("_", 1)
                layer_name = parts[0].upper() if len(parts) > 0 else "CLEANED"
                domain_name = parts[1] if len(parts) > 1 else "GENERAL_DATA"
                
                # Check if db filename has environment info
                cls._extract_env_from_filename(db_file.name, storage_info)
                
                # Create layer and domain
                layer_info = storage_info.get_or_create_layer(layer_name)
                domain_info = layer_info.get_or_create_domain(domain_name)
                
                # Connect to database and collect table information
                try:
                    conn = duckdb.connect(str(db_file))
                    tables = conn.execute("SHOW TABLES").fetchall()
                    
                    for table in tables:
                        table_name = table[0]
                        
                        # Try to extract environment from table name
                        cls._extract_env_from_table_name(table_name, storage_info)
                        
                        # Try to extract product and resolution from table name
                        # Convention: product_resolution
                        parts = table_name.split('_')
                        if len(parts) >= 2:
                            product_name = '_'.join(parts[:-1])
                            resolution = parts[-1]
                            
                            # Create source based on table name patterns
                            source_info = domain_info.get_or_create_source("duckdb")
                            product_info = source_info.get_or_create_product(product_name)
                            product_info.add_resolution(resolution)
                            product_info.file_count += 1
                            
                            # Get approximate row count for size estimation
                            try:
                                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                                # Rough size estimate
                                product_info.size_bytes += row_count * 100
                            except Exception as e:
                                console.print(f"[yellow]Warning: Could not get row count for table {table_name}: {e}[/yellow]")
                    
                    # Try to get date range for tables if available
                    if tables:
                        for table in tables:
                            table_name = table[0]
                            
                            # Check if the table has a date column
                            try:
                                # Get table schema
                                schema_info = conn.execute(f"DESCRIBE {table_name}").fetchall()
                                
                                # Look for env column in the schema
                                for col_info in schema_info:
                                    col_name = col_info[0]
                                    if col_name.lower() == 'env':
                                        try:
                                            env_value = conn.execute(f"SELECT DISTINCT {col_name} FROM {table_name} LIMIT 1").fetchone()
                                            if env_value and env_value[0]:
                                                storage_info.env = str(env_value[0]).upper()
                                        except:
                                            pass
                                
                                # Check for date-like columns
                                date_columns = []
                                for col_info in schema_info:
                                    col_name = col_info[0]
                                    col_type = col_info[1]
                                    
                                    # Identify date columns
                                    if any(date_type in col_type.lower() for date_type in ['date', 'time', 'timestamp']) or \
                                       any(date_name in col_name.lower() for date_name in ['date', 'time', 'timestamp', 'dt']):
                                        date_columns.append(col_name)
                                
                                # If we found date columns, try to get min and max dates
                                if date_columns:
                                    date_col = date_columns[0]
                                    min_date_result = conn.execute(f"SELECT MIN({date_col}) FROM {table_name}").fetchone()
                                    max_date_result = conn.execute(f"SELECT MAX({date_col}) FROM {table_name}").fetchone()
                                    
                                    if min_date_result[0] and max_date_result[0]:
                                        min_date = str(min_date_result[0]).split()[0]  # Get date part only
                                        max_date = str(max_date_result[0]).split()[0]  # Get date part only
                                        
                                        # Get product and update dates
                                        parts = table_name.split('_')
                                        if len(parts) >= 2:
                                            product_name = '_'.join(parts[:-1])
                                            source_info = domain_info.get_or_create_source("duckdb")
                                            product_info = source_info.get_or_create_product(product_name)
                                            product_info.update_dates(min_date)
                                            product_info.update_dates(max_date)
                            except Exception:
                                # Skip date extraction for this table
                                pass
                    
                    conn.close()
                except Exception as e:
                    # Unable to open database
                    console.print(f"[yellow]Warning: Could not open DuckDB file {db_file}: {e}[/yellow]")
        except Exception as e:
            console.print(f"[yellow]Warning: Error collecting DuckDB information: {e}[/yellow]")
        
        return storage_info
    
    @classmethod
    def _extract_env_from_filename(cls, filename, storage_info):
        """Extract environment information from the database filename."""
        # Check for env pattern like backtest_data.db, live_data.db, etc.
        env_patterns = ['backtest', 'live', 'paper', 'sandbox']
        for pattern in env_patterns:
            if pattern in filename.lower():
                storage_info.env = pattern.upper()
                return
    
    @classmethod
    def _extract_env_from_table_name(cls, table_name, storage_info):
        """Extract environment information from the table name."""
        # Check for env pattern in table name
        env_match = re.search(r'_?(backtest|live|paper|sandbox)_?', table_name.lower())
        if env_match:
            storage_info.env = env_match.group(1).upper() 