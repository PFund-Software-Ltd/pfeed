from pydantic import UUID4

from pfeed.enums import ExtractType
from pfeed.requests.base_request import BaseRequest
from pfeed.storages.storage_config import StorageConfig
from pfeed.io.io_config import IOConfig


class AlphaFundFeedRetrieveRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.retrieve

    user_id: UUID4 | None = None
    fund_name: str | None = None
    fund_id: UUID4 | None = None

    storage_config_for_retrieval: StorageConfig
    io_config_for_retrieval: IOConfig
