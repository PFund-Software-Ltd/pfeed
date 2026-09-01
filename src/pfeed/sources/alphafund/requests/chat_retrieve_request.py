from pydantic import UUID4

from pfeed.enums import ExtractType
from pfeed.io.io_config import IOConfig
from pfeed.requests.base_request import BaseRequest
from pfeed.storages.storage_config import StorageConfig


class AlphaFundChatFeedRetrieveRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.retrieve

    fund_id: UUID4 | None = None
    channel_id: UUID4 | None = None
    chat_id: UUID4 | None = None

    storage_config_for_retrieval: StorageConfig
    io_config_for_retrieval: IOConfig
