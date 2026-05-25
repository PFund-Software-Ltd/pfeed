# pyright: reportUnknownVariableType=false
from datetime import date
from typing import Any

from pfund.entities.products.asset_type import AssetType
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from pfeed.enums import DataAccessType, DataCategory, DataProviderType, DataType


class SourceMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    data_origin: HttpUrl
    data_categories: dict[DataCategory, dict[DataType, list[AssetType]]]
    provider_type: DataProviderType
    access_type: DataAccessType
    api_key_required: bool = False
    rate_limits: dict[str, Any] | None = None  # NOTE: not in use yet
    start_date: date | str | None = Field(
        default=None,
        description=(
            "Earliest date for which data is known to be available. Approximate is fine — "
            "dates with no data are skipped at fetch time. None means unknown/unbounded. "
            "Accepts an ISO 8601 string (e.g. '2020-01-01') or a date; stored as date."
        ),
    )
    docs_url: HttpUrl | None = None
    github_repo: HttpUrl | None = None
    is_repo_official: bool | None = None

    @field_validator("start_date", mode="before")
    @classmethod
    def _parse_start_date(cls, v: date | str | None) -> date | None:
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v

    @field_validator("data_categories", mode="before")
    @classmethod
    def _coerce_asset_types(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        market_data_key = DataCategory.MARKET_DATA
        for category, type_map in v.items():
            if category != market_data_key and category != market_data_key.value:
                continue
            if not isinstance(type_map, dict):
                continue
            for dtype, items in type_map.items():
                if not isinstance(items, list):
                    continue
                type_map[dtype] = [
                    AssetType(value=item) if isinstance(item, str) else item
                    for item in items
                ]
        return v
