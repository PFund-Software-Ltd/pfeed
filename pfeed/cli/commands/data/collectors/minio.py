from pathlib import Path
from typing import Optional

from rich.console import Console

from pfeed.cli.commands.data.collectors.base import BaseCollector
from pfeed.cli.commands.data.models import StorageInfo
from pfeed.cli.commands.data.parsers import MarketDataParser, NewsDataParser, GenericParser
from pfeed.enums import DataStorage

console = Console()

class MinioCollector(BaseCollector):
    """Collector for MinIO data."""
    
    @classmethod
    def collect(cls, storage_type: DataStorage) -> StorageInfo:
        """
        Collect information about data in MinIO storage.
        
        Args:
            storage_type: Must be MINIO
            
        Returns:
            StorageInfo object with collected information
        """
        if storage_type != DataStorage.MINIO:
            raise ValueError(f"Unsupported storage type: {storage_type}")
            
        storage_info = StorageInfo(name=storage_type.name, path="MinIO storage")
        
        try:
            # Get MinIO client
            minio_client = cls._get_minio_client()
            if not minio_client:
                return storage_info
                
            bucket_name = cls._get_bucket_name()
            endpoint = cls._get_endpoint()
            
            # Update storage path with actual endpoint information
            storage_info.path = f"{endpoint}/{bucket_name}"
            
            # List all objects
            try:
                objects = list(minio_client.list_objects(bucket_name, recursive=True))
                
                # Process each object
                for obj in objects:
                    if not obj.object_name.endswith(('.parquet', '.csv', '.json', '.arrow', '.delta')):
                        continue
                        
                    # Convert path string to Path object to use our existing parsers
                    path_parts = obj.object_name.split('/')
                    path_obj = Path('/'.join(path_parts))
                    
                    # Try parsing with different parsers in order of specificity
                    parsed = False
                    if MarketDataParser.can_parse(path_obj):
                        parsed = MarketDataParser.parse(path_obj, storage_info)
                    elif NewsDataParser.can_parse(path_obj):
                        parsed = NewsDataParser.parse(path_obj, storage_info)
                    elif GenericParser.can_parse(path_obj):
                        parsed = GenericParser.parse(path_obj, storage_info)
                        
                    # Update size if parsed successfully
                    if parsed:
                        # Find the last processed product and add size
                        for layer in storage_info.layers.values():
                            for domain in layer.domains.values():
                                for source in domain.sources.values():
                                    for product in source.products.values():
                                        if product.file_count > 0:
                                            # Add size information from the MinIO object
                                            product.size_bytes += obj.size
                                            break
            except Exception as e:
                console.print(f"[yellow]Warning: Error listing MinIO objects: {e}[/yellow]")
                
        except Exception as e:
            console.print(f"[yellow]Warning: Error collecting MinIO information: {e}[/yellow]")
            
        return storage_info
    
    @classmethod
    def _get_minio_client(cls):
        """Get the MinIO client from the storage class."""
        try:
            # Try importing minio directly first
            try:
                from minio import Minio
                from pfeed.config import get_config
                config = get_config()
                
                # Check if MinIO configuration exists in config
                minio_endpoint = getattr(config, 'minio_endpoint', None)
                minio_access_key = getattr(config, 'minio_access_key', None) 
                minio_secret_key = getattr(config, 'minio_secret_key', None)
                
                if minio_endpoint and minio_access_key and minio_secret_key:
                    # Strip http/https from endpoint if present
                    endpoint = minio_endpoint.replace('http://', '').replace('https://', '')
                    client = Minio(
                        endpoint,
                        access_key=minio_access_key,
                        secret_key=minio_secret_key,
                        secure=minio_endpoint.startswith('https')
                    )
                    return client
            except ImportError:
                # Minio not available, continue with other methods
                pass
                
            # Try getting from storage class
            from pfeed.storages.minio_storage import MinioStorage
            storage = MinioStorage()
            
            # Get the client from the storage instance
            # Note: The actual implementation depends on how MinioStorage is implemented
            # This is a common pattern - client might be directly accessible or through a method
            for attr_name in ['client', '_client']:
                if hasattr(storage, attr_name):
                    return getattr(storage, attr_name)
            
            # If client is not directly accessible, try through a method
            for method_name in ['get_client', '_get_client', 'client']:
                if hasattr(storage, method_name) and callable(getattr(storage, method_name)):
                    return getattr(storage, method_name)()
            
            # Try to access the client via inner attributes
            if hasattr(storage, '_minio_client'):
                return storage._minio_client
                
            # Try to create the client using storage attributes
            if hasattr(storage, '_endpoint') and hasattr(storage, '_access_key') and hasattr(storage, '_secret_key'):
                try:
                    from minio import Minio
                    client = Minio(
                        storage._endpoint,
                        access_key=storage._access_key,
                        secret_key=storage._secret_key,
                        secure=storage._endpoint.startswith('https')
                    )
                    return client
                except ImportError:
                    # Minio not available
                    pass
            
            # As a last resort, try to create a new client with boto3
            try:
                import boto3
                from pfeed.config import get_config
                
                config = get_config()
                if hasattr(config, 'minio_endpoint') and hasattr(config, 'minio_access_key') and hasattr(config, 'minio_secret_key'):
                    # Create client using boto3
                    return boto3.client(
                        's3',
                        endpoint_url=config.minio_endpoint,
                        aws_access_key_id=config.minio_access_key,
                        aws_secret_access_key=config.minio_secret_key
                    )
            except ImportError:
                # boto3 not available
                pass
            
            # If MinIO is not set up, log a quieter warning instead of showing to user
            # since this is likely intentional (user doesn't use MinIO)
            import logging
            logging.getLogger('pfeed').debug("MinIO client not found. This is normal if you don't use MinIO.")
            return None
        except Exception as e:
            # Log to debug level instead of showing warning
            import logging
            logging.getLogger('pfeed').debug(f"Error getting MinIO client: {e}")
            return None
    
    @classmethod
    def _get_bucket_name(cls) -> str:
        """Get the MinIO bucket name."""
        try:
            from pfeed.storages.minio_storage import MinioStorage
            storage = MinioStorage()
            
            # Try to get bucket name from storage instance
            if hasattr(storage, '_kwargs') and isinstance(storage._kwargs, dict):
                if 'bucket_name' in storage._kwargs:
                    return storage._kwargs['bucket_name']
            
            # Try other common attribute names
            for attr_name in ['bucket', 'bucket_name', '_bucket']:
                if hasattr(storage, attr_name):
                    return getattr(storage, attr_name)
            
            return "default-bucket"
        except Exception:
            return "default-bucket"
    
    @classmethod
    def _get_endpoint(cls) -> str:
        """Get the MinIO endpoint."""
        try:
            from pfeed.storages.minio_storage import MinioStorage
            storage = MinioStorage()
            
            # Try to get endpoint from storage instance
            if hasattr(storage, '_kwargs') and isinstance(storage._kwargs, dict):
                if 'endpoint_url' in storage._kwargs:
                    return storage._kwargs['endpoint_url']
            
            # Try other common attribute names
            for attr_name in ['endpoint', 'endpoint_url', '_endpoint']:
                if hasattr(storage, attr_name):
                    return getattr(storage, attr_name)
            
            return "minio-endpoint"
        except Exception:
            return "minio-endpoint" 