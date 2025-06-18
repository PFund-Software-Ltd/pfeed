from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct

import os
from abc import ABC, abstractmethod

from pfeed.enums import DataSource, DataProviderType, DataAccessType, DataCategory
from pfeed.const.aliases import ALIASES 


class BaseSource(ABC):
    name: ClassVar[DataSource]
    
    def __init__(self, api_key: str | None=None):
        self.generic_metadata, self.specific_metadata = self._load_metadata()
        self._api_key: str | None = api_key or self._get_api_key()
        self.start_date: str | None = self.specific_metadata.get('start_date', None)
        self.access_type = DataAccessType[self.generic_metadata['access_type'].upper()]
        self.provider_type = DataProviderType[self.generic_metadata['provider_type'].upper()]
        self.data_origin = self.generic_metadata['data_origin']
        self.rate_limits = self.generic_metadata['rate_limits']
        self.docs_url = self.generic_metadata['docs_url']
        self.data_categories: list[DataCategory] = [DataCategory[category.upper()] for category in self.generic_metadata['data_categories'].keys()]
        
    @abstractmethod
    def create_product(self, basis: str, symbol: str='', **specs) -> BaseProduct:
        pass
    
    def _get_api_key(self) -> str | None:
        alias = next((alias for alias, name in ALIASES.items() if name.upper() == self.name), self.name)
        api_key_name, api_key_alias = f'{self.name}_API_KEY', f'{alias}_API_KEY'
        api_key: str | None = os.getenv(api_key_name) or os.getenv(api_key_alias)
        is_api_key_required = self.generic_metadata['api_key_required']
        if is_api_key_required and not api_key:
            api_key_name_choices = [api_key_name] if api_key_name == api_key_alias else [api_key_name, api_key_alias]
            raise ValueError(f'{" or ".join(api_key_name_choices)} is not set')
        return api_key
    
    def _load_metadata(self):
        import os
        import yaml
        metadata_file_path = os.path.join(os.path.dirname(__file__), self.name.lower(), 'metadata.yml')
        with open(metadata_file_path, 'r') as file:
            metadata_docs = yaml.safe_load_all(file)
            generic_metadata = next(metadata_docs)
            specific_metadata = next(metadata_docs, {})
        return generic_metadata, specific_metadata
