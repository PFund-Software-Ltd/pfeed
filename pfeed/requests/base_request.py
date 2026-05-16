from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from pfeed.enums import ExtractType, DataLayer
from pfeed.storages.storage_config import StorageConfig
from pfeed._io.io_config import IOConfig


class BaseRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    data_origin: str = ''
    data_domain: str
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
        from pfund_kit.style import RichColor, TextStyle, cprint
        super().model_post_init(__context)
        storage_config = self.storage_config
        if storage_config:
            is_raw_data = storage_config.data_layer == DataLayer.RAW
            if self.extract_type != ExtractType.retrieve:
                self.clean_data = not is_raw_data
            else:
                # if it's not retrieving raw data, there's nothing to clean, clean_data is always False
                if not is_raw_data and self.clean_data:
                    cprint(
                        f'"clean_data" parameter is ignored when data layer is not RAW (got data layer={storage_config.data_layer})',
                        style=TextStyle.BOLD + RichColor.YELLOW
                    )
                    self.clean_data = False
            if not storage_config.data_domain:
                storage_config.data_domain = self.data_domain
            else:
                if storage_config.data_layer != DataLayer.CURATED:
                    raise ValueError(f'Custom data_domain={storage_config.data_domain} is only allowed when data layer is CURATED, but got data_layer={storage_config.data_layer}')
