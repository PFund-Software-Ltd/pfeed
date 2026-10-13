from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    from pfund.entities.products.product_base import BaseProduct

    from pfeed.base.feed import BaseFeed

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
    url: HttpUrl
    data_categories: dict[DataCategory, dict[DataType, list[AssetType]]]
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


class BaseSource(ABC):
    METADATA: ClassVar[SourceMetadata]
    Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]]

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        # attach this source class to its feeds, e.g. BybitMarketFeed.DataSource = Bybit,
        # so classmethods like create_data_model() can reach the source without an instance.
        # done here instead of declaring DataSource = Bybit on the feed to avoid a circular import:
        # the source module imports its feeds at runtime to build Feeds, so a feed can't import its source back
        for data_category, Feed in cls.__dict__.get("Feeds", {}).items():
            if data_category != Feed.data_domain:
                raise TypeError(
                    f"{cls.__name__}.Feeds lists {Feed.__name__} under {data_category}, "
                    f"but it is a {Feed.data_domain} feed"
                )
            if "DataSource" in Feed.__dict__ and Feed.DataSource is not cls:
                raise TypeError(
                    f"{Feed.__name__} already belongs to {Feed.DataSource.__name__}, "
                    f"{cls.__name__} can't list it in its Feeds"
                )
            Feed.DataSource = cls

    def __init__(
        self,
        pipeline_mode: bool = False,
        num_workers: int | dict[DataCategory | str, int] | None = None,
    ):
        self._api_key: str | None = self.api_access.get_key() if self.api_access else None
        self._pipeline_mode: bool = pipeline_mode
        self._feeds: list[BaseFeed] = []
        if isinstance(num_workers, dict):
            num_workers = {DataCategory[k.upper()]: v for k, v in num_workers.items()}
        self._num_workers: int | dict[DataCategory | str, int] | None = num_workers
        self._create_feeds()

    @property
    def name(self) -> str:
        return self.METADATA.name

    @property
    def data_categories(self) -> dict[DataCategory, dict[DataType, list[AssetType]]]:
        return self.METADATA.data_categories

    @property
    def api_access(self) -> APIAccess | None:
        return self.METADATA.api_access

    @property
    def feeds(self) -> list[BaseFeed]:
        return self._feeds

    def is_pipeline(self) -> bool:
        return self._pipeline_mode

    def _create_feeds(self):
        for data_category, Feed in self.Feeds.items():
            num_workers: int | None = (
                self._num_workers.get(data_category, None)
                if isinstance(self._num_workers, dict)
                else self._num_workers
            )
            feed: BaseFeed = Feed(data_source=self, pipeline_mode=self._pipeline_mode, num_workers=num_workers)
            if feed not in self._feeds:
                self._feeds.append(feed)
            # dynamically set attributes e.g. self.market_feed
            setattr(self, data_category.feed_name, feed)

    def get_data_categories(self) -> list[DataCategory]:
        return list(self.METADATA.data_categories.keys())

    @classmethod
    def create_product(cls, basis: str, symbol: str = "", **specs: Any) -> BaseProduct:
        raise NotImplementedError(f"{cls.METADATA.name} does not support creating products")
