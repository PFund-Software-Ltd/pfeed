from __future__ import annotations
from typing import Any

from pfeed.enums import DataSource
from pfeed.sources.base_source import BaseSource
from pfeed.data_models.base_data_model import BaseMetadataModel


class DataProviderMetadataModelMixin:
    data_source: DataSource


# for typing only
class DataProviderMetadataModel(DataProviderMetadataModelMixin, BaseMetadataModel):
    pass
    

class DataProviderModelMixin:
    """Mixin for data models that source data from external data providers.
    
    This mixin adds data provider-specific functionality to data models, including:
    - BaseSource integration for accessing provider APIs (batch/stream)
    - Automatic data_origin defaulting from the source name
    - String representation that includes source and origin information
    - Metadata generation that includes data_source field
    
    Use this mixin for models that represent data from external providers
    (e.g., Bybit, Databento, Yahoo Finance). Do NOT use for internal data
    sources (e.g., pfund engine data) - those should inherit directly from
    BaseDataModel.
    
    Example:
        class MarketDataModel(DataProviderModelMixin, BaseDataModel):
            # Now has data_source: BaseSource and related methods
            ...
    
    Note:
        Must be inherited before BaseDataModel to ensure proper MRO.
    """
    data_source: BaseSource

    def model_post_init(self: DataProviderMetadataModel, __context: Any) -> None:
        super().model_post_init(__context)
        if not self.data_origin:
            self.data_origin = self.data_source.name

    def is_data_origin_effective(self: DataProviderMetadataModel) -> bool:
        """
        A data_origin is not effective if it is the same as the source name.
        """
        return self.data_origin != self.data_source.name

    def __str__(self: DataProviderMetadataModel) -> str:
        if self.is_data_origin_effective():
            return f"{self.data_source.name}:{self.data_origin}"
        else:
            return f"{self.data_source.name}"
    
    def to_metadata(self: DataProviderMetadataModel, **fields) -> DataProviderMetadataModel:
        return super().to_metadata(
            data_source=self.data_source.name,
            **fields,
        )
