from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.enums import DataCategory
    from pfeed.feeds.base_feed import BaseFeed


def get_feed(
    data_source: str,
    data_category: DataCategory | str,
    pipeline_mode: bool = False,
    num_workers: int | None = None,
) -> BaseFeed:
    from pfeed import registry

    Feed = registry.get_feed(data_source, data_category)
    return Feed(pipeline_mode=pipeline_mode, num_workers=num_workers)
