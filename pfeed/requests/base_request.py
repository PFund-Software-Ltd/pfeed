from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from pfeed.enums import ExtractType, DataLayer
from pfeed.storages.storage_config import StorageConfig


class BaseRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    data_origin: str = ''
    extract_type: ExtractType
    storage_config: StorageConfig | None = None
    is_loaded: bool = Field(default=False, description='Whether load() has been called for the request')
    clean_data: bool = Field(
        default=True,
        description="""
            Whether to clean raw data after download when storage_config is None.
            When storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
            If True, downloaded raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
            If False, downloaded raw data will be returned as is.
        """
    )

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def set_loaded(self) -> None:
        '''Set the request as loaded'''
        self.is_loaded = True

    def is_streaming(self) -> bool:
        return False

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.storage_config and self.extract_type != ExtractType.retrieve:
            self.clean_data = self.storage_config.data_layer != DataLayer.RAW