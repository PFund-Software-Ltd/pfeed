from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from pfeed.enums import ExtractType, DataLayer
from pfeed.storages.storage_config import StorageConfig
from pfeed._io.io_config import IOConfig


class BaseRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    data_origin: str = ''
    extract_type: ExtractType
    storage_config: StorageConfig | None = None
    io_config: IOConfig | None = None
    clean_data: bool = Field(
        default=True,
        description="""
            Whether to clean raw data after download when storage_config is None.
            When storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
            If True, downloaded raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
            If False, downloaded raw data will be returned as is.
        """
    )

    def __hash__(self) -> int:
        return hash(id(self))

    def __eq__(self, other: object) -> bool:
        return self is other

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def is_streaming(self) -> bool:
        return False

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        storage_config = self.storage_config
        if storage_config:
            is_raw_data = storage_config.data_layer == DataLayer.RAW
            if self.extract_type != ExtractType.retrieve:
                self.clean_data = not is_raw_data
            else:
                # if it's not retrieving raw data, there's nothing to clean, clean_data is always False
                if not is_raw_data:
                    self.clean_data = False
