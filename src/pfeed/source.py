from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

import os
from abc import ABC
from datetime import date

from pfund.entities.products.asset_type import AssetType
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    model_validator,
)

from pfeed.enums import DataAccessType, DataCategory, DataProviderType, DataType


class APIAccess(BaseModel):
    model_config = ConfigDict(frozen=True)

    # env var to load the API key from, e.g. "DATABENTO_API_KEY"; None means the API doesn't use a key
    key_name: str | None = None
    key_required: bool = False
    rate_limits: dict[str, Any] | None = None  # NOTE: not in use yet

    @model_validator(mode="after")
    def _check_key(self) -> Self:
        if self.key_required and self.key_name is None:
            raise ValueError("key_required is True but key_name is not set")
        return self

    def get_key(self) -> str | None:
        if self.key_name is None:
            return None
        key: str | None = os.getenv(self.key_name)
        if self.key_required and not key:
            raise ValueError(f"{self.key_name} is not set")
        return key


class SourceMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    data_origin: HttpUrl
    data_categories: dict[DataCategory, dict[DataType, list[AssetType]]]
    # verbs each feed supports, e.g. {MARKET_DATA: {"download", "stream"}}; each feed defines and gates on its own verbs
    feed_capabilities: dict[DataCategory, frozenset[str]]
    provider_type: DataProviderType
    access_type: DataAccessType
    api_access: APIAccess | None = None
    start_date: date | None = Field(
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

    @model_validator(mode="after")
    def _check_feed_capabilities(self) -> Self:
        if unknown := self.feed_capabilities.keys() - self.data_categories.keys():
            raise ValueError(f"feed_capabilities has categories not in data_categories: {sorted(unknown)}")
        return self


class BaseSource(ABC):
    METADATA: ClassVar[SourceMetadata]

    def __init__(self):
        self._api_key: str | None = self.api_access.get_key() if self.api_access else None

    @property
    def name(self) -> str:
        return self.METADATA.name

    @property
    def api_access(self) -> APIAccess | None:
        return self.METADATA.api_access

    def get_data_categories(self) -> list[DataCategory]:
        return list(self.METADATA.data_categories.keys())

    def create_product(self, basis: str, symbol: str = "", **specs: Any) -> BaseProduct:
        raise NotImplementedError(f"{self.name} does not support creating products")
