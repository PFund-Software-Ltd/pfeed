from pfeed.enums import ExtractType
from pfeed.sources.alphafund.requests.agent_base_request import (
    AlphaFundAgentFeedBaseRequest,
)
from pfeed.storages.storage_config import StorageConfig
from pfeed.io.io_config import IOConfig


class AlphaFundAgentFeedDownloadRequest(AlphaFundAgentFeedBaseRequest):
    extract_type: ExtractType = ExtractType.download
    storage_config: StorageConfig  # pyright: ignore[reportGeneralTypeIssues]
    io_config: IOConfig  # pyright: ignore[reportGeneralTypeIssues]
