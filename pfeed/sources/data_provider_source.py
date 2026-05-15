# pyright: reportUnusedParameter=false, reportUnknownParameterType=false
from __future__ import annotations
from typing import TYPE_CHECKING, Any, ClassVar
if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

import os

from pfeed.sources.base_source import BaseSource
from pfeed.sources.source_metadata import SourceMetadata
from pfeed.enums import DataCategory


class DataProviderSource(BaseSource):
    METADATA: ClassVar[SourceMetadata]
    
    def __init__(self):
        self._batch_api: Any | None = None
        self._stream_api: Any | None = None
        self._api_key: str | None = self._get_api_key()
    
    def get_data_categories(self) -> list[DataCategory]:
        return list(self.METADATA.data_categories.keys())
    
    def get_batch_api(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(f'{self.name} does not support getting batch API')
    
    def get_stream_api(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(f'{self.name} does not support getting stream API')
    
    def create_product(self, basis: str, symbol: str='', **specs: Any) -> BaseProduct:
        raise NotImplementedError(f'{self.name} does not support creating products')

    def _get_api_key(self) -> str | None:
        from pfeed.utils.aliases import ALIASES
        alias = ALIASES.resolve(self.name)
        api_key_name, api_key_alias = f'{self.name}_API_KEY', f'{alias}_API_KEY'
        api_key: str | None = os.getenv(api_key_name) or os.getenv(api_key_alias)
        is_api_key_required = self.METADATA.api_key_required
        if is_api_key_required and not api_key:
            api_key_name_choices = [api_key_name] if api_key_name == api_key_alias else [api_key_name, api_key_alias]
            raise ValueError(f'{" or ".join(api_key_name_choices)} is not set')
        return api_key
