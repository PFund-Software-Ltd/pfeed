"""Registry of data sources, discovered via the "pfeed.sources" entry points.

Every data source is a plugin package `pfeed-xxx` (e.g. pfeed-bybit), registering
an entry point named after the source, pointing at its source class:
    [project.entry-points."pfeed.sources"]
    bybit = "pfeed_bybit:Bybit"
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from importlib.metadata import EntryPoint

    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.source import BaseSource

from functools import cache
from importlib.metadata import entry_points

from pfeed.enums import DataCategory

ENTRY_POINT_GROUP = "pfeed.sources"


@cache
def get_entry_points() -> dict[str, EntryPoint]:
    """Returns {source name: entry point} of all installed data sources, without importing them."""
    eps: dict[str, EntryPoint] = {}
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        name = ep.name.upper()
        if name in eps:
            dists = [e.dist.name if e.dist else "unknown" for e in (eps[name], ep)]
            raise ValueError(f"data source {name} is registered by multiple packages: {dists}")
        eps[name] = ep
    return eps


def list_sources() -> list[str]:
    return sorted(get_entry_points())


def get_entry_point(name: str) -> EntryPoint:
    try:
        return get_entry_points()[name.upper()]
    except KeyError:
        raise ValueError(f"unknown data source '{name}', installed data sources: {list_sources()}") from None


def get_source(name: str) -> type[BaseSource]:
    ep = get_entry_point(name)
    Source: type[BaseSource] = ep.load()
    if Source.METADATA.name != ep.name.upper():
        raise ValueError(
            f"entry point '{ep.name}' ({ep.value}) loads a source whose name is {Source.METADATA.name}"
        )
    return Source


def get_feed(name: str, data_category: DataCategory | str) -> type[BaseFeed]:
    Source = get_source(name)
    data_category = DataCategory[data_category.upper()]
    try:
        return Source.Feeds[data_category]
    except KeyError:
        raise ValueError(
            f"data source {name.upper()} has no feed for {data_category}, available: {list(Source.Feeds)}"
        ) from None
