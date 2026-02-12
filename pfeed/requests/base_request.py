from pydantic import BaseModel, ConfigDict

from pfeed.enums import ExtractType
from pfeed.storages.storage_config import StorageConfig


class BaseRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    data_origin: str = ''
    extract_type: ExtractType
    storage_config: StorageConfig | None = None

    @property
    def name(self) -> str:
        return self.__class__.__name__
