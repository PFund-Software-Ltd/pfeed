from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct

import os
from abc import ABC, abstractmethod

from pfeed.enums import DataSource, DataProviderType, DataAccessType, DataCategory
from pfeed.aliases import ALIASES 


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
    
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        assert hasattr(cls, 'name'), f'{cls.__name__} must have a name attribute'
        
    @abstractmethod
    def create_product(self, basis: str, symbol: str='', **specs) -> BaseProduct:
        pass
    
    def _get_api_key(self) -> str | None:
        alias = ALIASES.resolve(self.name)
        api_key_name, api_key_alias = f'{self.name}_API_KEY', f'{alias}_API_KEY'
        api_key: str | None = os.getenv(api_key_name) or os.getenv(api_key_alias)
        is_api_key_required = self.generic_metadata['api_key_required']
        if is_api_key_required and not api_key:
            api_key_name_choices = [api_key_name] if api_key_name == api_key_alias else [api_key_name, api_key_alias]
            raise ValueError(f'{" or ".join(api_key_name_choices)} is not set')
        return api_key
    
    def _load_metadata(self):
        from pfund_kit.utils.yaml import load
        from pfeed import get_config
        config = get_config()
        metadata_file_path = config._paths.package_path / 'sources' / self.name.lower() / 'metadata.yml'
        docs = load(metadata_file_path, multi_document=True)
        generic_metadata = docs[0] if docs else {}
        specific_metadata = docs[1] if len(docs) > 1 else {} if docs else {}
        return generic_metadata, specific_metadata
