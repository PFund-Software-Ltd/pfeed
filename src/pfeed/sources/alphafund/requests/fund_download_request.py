from pydantic import UUID4

from pfeed.enums import ExtractType
from pfeed.requests.base_request import BaseRequest
from pfeed.storages.storage_config import StorageConfig
from pfeed.io.io_config import IOConfig


class AlphaFundFeedDownloadRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.download

    user_id: UUID4
    fund_name: str
    fund_id: UUID4 | None = None

    storage_config: StorageConfig  # pyright: ignore[reportGeneralTypeIssues]
    io_config: IOConfig  # pyright: ignore[reportGeneralTypeIssues]
