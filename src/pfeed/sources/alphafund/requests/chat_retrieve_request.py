from pfeed.enums import ExtractType
from pfeed.io.io_config import IOConfig
from pfeed.sources.alphafund.requests.chat_base_request import (
    AlphaFundChatFeedBaseRequest,
)
from pfeed.storages.storage_config import StorageConfig


class AlphaFundChatFeedRetrieveRequest(AlphaFundChatFeedBaseRequest):
    extract_type: ExtractType = ExtractType.retrieve
    storage_config_for_retrieval: StorageConfig
    io_config_for_retrieval: IOConfig
