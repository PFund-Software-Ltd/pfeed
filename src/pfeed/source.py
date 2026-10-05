from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

import os
from abc import ABC, abstractmethod
from datetime import date

from pfund.entities.products.asset_type import AssetType
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

from pfeed.enums import DataAccessType, DataCategory, DataProviderType, DataType


class SourceMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    data_origin: HttpUrl
    data_categories: dict[DataCategory, dict[DataType, list[AssetType]]]
    # verbs each feed supports, e.g. {MARKET_DATA: {"download", "stream"}}; each feed defines and gates on its own verbs
    feed_capabilities: dict[DataCategory, frozenset[str]]
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

    @model_validator(mode="after")
    def _check_feed_capabilities(self) -> Self:
        if unknown := self.feed_capabilities.keys() - self.data_categories.keys():
            raise ValueError(f"feed_capabilities has categories not in data_categories: {sorted(unknown)}")
        return self


class BaseSource(ABC):
    name: ClassVar[str]

    def __init__(self):
        self._batch_api: Any | None = None
        self._stream_api: Any | None = None

    @abstractmethod
    def get_data_categories(self) -> list[DataCategory]:
        pass

    def get_batch_api(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(f"{self.name} does not support getting batch API")

    def get_stream_api(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(f"{self.name} does not support getting stream API")


class DataProviderSource(BaseSource):
    METADATA: ClassVar[SourceMetadata]

    def __init__(self):
        super().__init__()
        self._api_key: str | None = self._get_api_key()

    def get_data_categories(self) -> list[DataCategory]:
        return list(self.METADATA.data_categories.keys())

    def create_product(self, basis: str, symbol: str = "", **specs: Any) -> BaseProduct:
        raise NotImplementedError(f"{self.name} does not support creating products")

    def _get_api_key(self) -> str | None:
        api_key_name = f"{self.name}_API_KEY"
        api_key: str | None = os.getenv(api_key_name)
        if self.METADATA.api_key_required and not api_key:
            raise ValueError(f"{api_key_name} is not set")
        return api_key
