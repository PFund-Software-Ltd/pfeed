from pydantic import BaseModel, ConfigDict

from pfeed.enums import ExtractType


class BaseRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid', frozen=True)

    data_origin: str = ''
    request_type: ExtractType