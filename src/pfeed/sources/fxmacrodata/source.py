from __future__ import annotations

from typing import ClassVar

from pfeed.enums import DataAccessType, DataCategory, DataProviderType
from pfeed.source import DataProviderSource, SourceMetadata


class FXMacroDataSource(DataProviderSource):
    """FXMacroData provider metadata and optional credential handling."""

    name: ClassVar[str] = "FXMACRODATA"
    METADATA: ClassVar[SourceMetadata] = SourceMetadata(
        url="https://api.fxmacrodata.com",
        data_categories={DataCategory.ANNOUNCEMENT_DATA: {}},
        provider_type=DataProviderType.VENDOR,
        access_type=DataAccessType.FREE_TIER,
        api_key_required=False,
        docs_url="https://fxmacrodata.com/documentation/reference",
    )
