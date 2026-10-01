from pydantic import UUID4

from pfeed.enums import ExtractType
from pfeed.io.io_config import IOConfig
from pfeed.requests.base_request import BaseRequest
from pfeed.storages.storage_config import StorageConfig


class AlphaFundAgentFeedRetrieveRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.retrieve

    fund_id: UUID4 | None = None
    agent_name: str | None = None
    agent_role: str | None = None
    agent_id: UUID4 | None = None

    storage_config_for_retrieval: StorageConfig
    io_config_for_retrieval: IOConfig
