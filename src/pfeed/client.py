from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.source import BaseSource

from abc import ABC

from pfeed.enums import DataCategory


class DataClient(ABC):
    DataSource: ClassVar[type[BaseSource]]
    Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]]

    def __init__(
        self,
        pipeline_mode: bool = False,
        num_workers: int | dict[DataCategory | str, int] | None = None,
    ):
        self._pipeline_mode: bool = pipeline_mode
        self.data_source: BaseSource = self.DataSource()
        self._feeds: list[BaseFeed] = []
        if isinstance(num_workers, dict):
            num_workers = {DataCategory[k.upper()]: v for k, v in num_workers.items()}
        self._num_workers: int | dict[DataCategory | str, int] | None = num_workers
        self._create_feeds()

    @property
    def name(self) -> str:
        return self.data_source.name

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
            feed: BaseFeed = Feed(pipeline_mode=self._pipeline_mode, num_workers=num_workers)
            if feed not in self._feeds:
                self._feeds.append(feed)
            # dynamically set attributes e.g. self.market_feed
            setattr(self, data_category.feed_name, feed)
