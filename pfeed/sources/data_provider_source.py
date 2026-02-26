from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar
if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

import os

from pfeed.enums import DataProviderType, DataAccessType, DataCategory, DataSource
from pfeed.sources.base_source import BaseSource


class DataProviderSource(BaseSource):
    name: ClassVar[DataSource]
    
    def __init__(self):
        self.generic_metadata: dict[str, Any]
        self.specific_metadata: dict[str, Any]
        self.generic_metadata, self.specific_metadata = self._load_metadata()
        super().__init__(data_categories=[DataCategory[category.upper()] for category in self.generic_metadata['data_categories'].keys()])
        self._api_key: str | None = self._get_api_key()
        self.start_date: str | None = self.specific_metadata.get('start_date', None)
        self.access_type: DataAccessType = DataAccessType[self.generic_metadata['access_type'].upper()]
        self.provider_type: DataProviderType = DataProviderType[self.generic_metadata['provider_type'].upper()]
        self.data_origin: str = self.generic_metadata['data_origin']
        self.rate_limits: Any = self.generic_metadata['rate_limits']
        self.docs_url: str | None = self.generic_metadata['docs_url']

    def create_product(self, basis: str, symbol: str='', **specs: Any) -> BaseProduct:  # pyright: ignore[reportUnusedParameter]
        raise NotImplementedError(f'{self.name} does not support creating products')

    def _get_api_key(self) -> str | None:
        from pfeed.utils.aliases import ALIASES
        alias = ALIASES.resolve(self.name)
        api_key_name, api_key_alias = f'{self.name}_API_KEY', f'{alias}_API_KEY'
        api_key: str | None = os.getenv(api_key_name) or os.getenv(api_key_alias)
        is_api_key_required = self.generic_metadata['api_key_required']
        if is_api_key_required and not api_key:
            api_key_name_choices = [api_key_name] if api_key_name == api_key_alias else [api_key_name, api_key_alias]
            raise ValueError(f'{" or ".join(api_key_name_choices)} is not set')
        return api_key

    def _load_metadata(self) -> tuple[dict[str, Any], dict[str, Any]]:
        from pfund_kit.utils.yaml import load
        from pfeed import get_config
        config = get_config()
        metadata_file_path = config._paths.package_path / 'sources' / self.name.lower() / 'metadata.yml'
        docs = load(metadata_file_path, multi_document=True)
        generic_metadata: dict[str, Any] = docs[0] if docs else {}  # pyright: ignore[reportAssignmentType]
        specific_metadata: dict[str, Any] = docs[1] if docs and len(docs) > 1 else {}  # pyright: ignore[reportAssignmentType]
        return generic_metadata, specific_metadata
