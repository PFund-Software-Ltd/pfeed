"""Registry of data sources, discovered via the "pfeed.sources" entry points.

Every data source is registered by a package as an entry point named after the source,
pointing at its source class. Usually it's a plugin package `pfeed-xxx` (e.g. pfeed-bybit):
    [project.entry-points."pfeed.sources"]
    bybit = "pfeed_bybit:Bybit"
or a package shipping its own source inside (e.g. pfund):
    [project.entry-points."pfeed.sources"]
    pfund = "pfund._pfeed_source:PFund"
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pfund_blocks.enums import DataCategory

    from pfeed.base.feed import BaseFeed
    from pfeed.source import BaseSource

from pfund_kit.plugins import PluginRegistry

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


def get_feed_class(name: str, data_category: DataCategory | str) -> type[BaseFeed]:
    Source = get_source(name)
    # keys of Source.Feeds are StrEnum members, which hash and compare equal to their string values,
    # so a string looks up any source's categories, e.g. pfund's "COMPONENT_DATA" too
    Feeds = cast("dict[str, type[BaseFeed]]", Source.Feeds)
    data_category = data_category.upper()
    try:
        return Feeds[data_category]
    except KeyError:
        raise ValueError(
            f"data source {name.upper()} has no feed for {data_category}, available: {list(Source.Feeds)}"
        ) from None


def get_feed(
    data_source: str,
    data_category: DataCategory | str,
    pipeline_mode: bool = False,
    num_workers: int | None = None,
) -> BaseFeed:
    """Creates the feed of a data source's data category, e.g. pe.get_feed("bybit", "market_data")."""
    Feed = get_feed_class(data_source, data_category)
    # feeds are only created by their source, so build the source and return its feed
    source = get_source(data_source)(
        pipeline_mode=pipeline_mode, num_workers=num_workers
    )
    from pfeed.source import get_feed_name

    return getattr(source, get_feed_name(Feed.data_domain))
