from __future__ import annotations

from pfeed.enums import ExtractType
from pfeed.sources.alphafund.requests.chat_base_request import (
    AlphaFundChatFeedBaseRequest,
)
from pfeed.storages.storage_config import StorageConfig
from pfeed.io.io_config import IOConfig


class AlphaFundChatFeedDownloadRequest(AlphaFundChatFeedBaseRequest):
    extract_type: ExtractType = ExtractType.download
    storage_config: StorageConfig  # pyright: ignore[reportGeneralTypeIssues]
    io_config: IOConfig  # pyright: ignore[reportGeneralTypeIssues]
