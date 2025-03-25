from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct
    from pfeed.typing import tDATA_SOURCE

import os
from abc import ABC, abstractmethod

from pfund.utils.utils import Singleton
from pfeed.enums import DataSource, DataProviderType, DataAccessType, ProductType
from pfeed.const.aliases import BIDIRECTIONAL_ALIASES 


class BaseSource(Singleton, ABC):
    def __init__(self, name: tDATA_SOURCE, api_key: str | None=None):
        self.name = DataSource[name.upper()]
        api_key_name, api_key_alias = f'{self.name}_API_KEY', f'{BIDIRECTIONAL_ALIASES.get(self.name, self.name)}_API_KEY'
        self._api_key = api_key or os.getenv(api_key_name) or os.getenv(api_key_alias)
        self.generic_metadata, self.specific_metadata = self._load_metadata()
        self.start_date = self.specific_metadata.get('start_date', None)
        self.api_key_required = self.generic_metadata['api_key_required']
        if self.api_key_required and not self._api_key:
            raise ValueError(f'{api_key_name} or {api_key_alias} is not set')
        self.access_type = DataAccessType[self.generic_metadata['access_type'].upper()]
        self.provider_type = DataProviderType[self.generic_metadata['provider_type'].upper()]
        self.data_origin = self.generic_metadata['data_origin']
        self.rate_limits = self.generic_metadata['rate_limits']
        self.docs_url = self.generic_metadata['docs_url']
        self.data_categories = list(self.generic_metadata['data_categories'].keys())
        has_market_data = 'market_data' in self.data_categories
        if has_market_data:
            self.highest_resolution, self.lowest_resolution = self._get_highest_and_lowest_resolutions()
            self.ptypes = self.product_types = self._extract_product_types()
    
    @abstractmethod
    def create_product(self, product_basis: str, symbol: str='', **product_specs) -> BaseProduct:
        pass
    
    def _get_highest_and_lowest_resolutions(self):
        from pfund.datas.resolution import Resolution
        from pfeed.enums import MarketDataType
        data_types = [MarketDataType[data_type.upper()] for data_type in self.generic_metadata['data_categories']['market_data']]
        resolutions = sorted([Resolution(data_type) for data_type in data_types], reverse=True)
        return resolutions[0], resolutions[-1]
    
    def _load_metadata(self):
        import os
        import yaml
        metadata_file_path = os.path.join(os.path.dirname(__file__), self.name.lower(), 'metadata.yml')
        with open(metadata_file_path, 'r') as file:
            metadata_docs = yaml.safe_load_all(file)
            generic_metadata = next(metadata_docs)
            specific_metadata = next(metadata_docs, {})
        return generic_metadata, specific_metadata

    def _extract_product_types(self) -> list[ProductType]:
        return list(set(
            ProductType[product_type.upper()]
            for data_type in self.generic_metadata['data_categories']['market_data']
            for product_type in self.generic_metadata['data_categories']['market_data'][data_type]
        ))
