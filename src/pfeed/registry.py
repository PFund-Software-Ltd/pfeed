"""Registry of data sources, discovered via the "pfeed.sources" entry points.

Every data source is a plugin package `pfeed-xxx` (e.g. pfeed-bybit), registering
an entry point named after the source, pointing at its source class:
    [project.entry-points."pfeed.sources"]
    bybit = "pfeed_bybit:Bybit"
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.base.feed import BaseFeed
    from pfeed.source import BaseSource

from pfund_kit.plugins import PluginRegistry

from pfeed.enums import DataCategory

ENTRY_POINT_GROUP = "pfeed.sources"
plugin_registry = PluginRegistry(ENTRY_POINT_GROUP, kind="data source")
get_entry_points = plugin_registry.get_entry_points
list_sources = plugin_registry.list_names
get_entry_point = plugin_registry.get_entry_point


def get_source(name: str) -> type[BaseSource]:
    ep = get_entry_point(name)
    Source: type[BaseSource] = ep.load()
    if Source.metadata.name != ep.name.upper():
        raise ValueError(
            f"entry point '{ep.name}' ({ep.value}) loads a source whose name is {Source.metadata.name}"
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
