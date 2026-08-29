from pfeed.enums import ExtractType
from pfeed.io.io_config import IOConfig
from pfeed.sources.alphafund.requests.agent_base_request import (
    AlphaFundAgentFeedBaseRequest,
)
from pfeed.storages.storage_config import StorageConfig


class AlphaFundAgentFeedRetrieveRequest(AlphaFundAgentFeedBaseRequest):
    extract_type: ExtractType = ExtractType.retrieve
    storage_config_for_retrieval: StorageConfig
    io_config_for_retrieval: IOConfig
