from pydantic import UUID4

from pfeed.enums import ExtractType
from pfeed.io.io_config import IOConfig
from pfeed.requests.base_request import BaseRequest
from pfeed.storages.storage_config import StorageConfig


class AlphaFundAgentFeedDownloadRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.download

    fund_id: UUID4
    agent_name: str
    agent_role: str
    agent_class: str
    agent_id: UUID4 | None = None

    storage_config: StorageConfig  # pyright: ignore[reportGeneralTypeIssues]
    io_config: IOConfig  # pyright: ignore[reportGeneralTypeIssues]
