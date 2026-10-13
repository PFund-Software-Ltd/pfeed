from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.base.feed import BaseFeed
    from pfeed.enums import DataCategory


def get_feed(
    data_source: str,
    data_category: DataCategory | str,
    pipeline_mode: bool = False,
    num_workers: int | None = None,
) -> BaseFeed:
    from pfeed import registry

    Feed = registry.get_feed(data_source, data_category)
    # feeds are only created by their source, so build the source and return its feed
    source = registry.get_source(data_source)(pipeline_mode=pipeline_mode, num_workers=num_workers)
    return getattr(source, Feed.data_domain.feed_name)
