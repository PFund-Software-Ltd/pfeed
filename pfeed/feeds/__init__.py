from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDATA_SOURCE, tDATA_TOOL
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.feeds.market_feed import MarketFeed


def get_Feed(data_source: tDATA_SOURCE) -> type[BaseFeed]:
    from pfeed.enums import DataSource
    data_source = DataSource[data_source.upper()]
    DataFeed = data_source.feed_class
    assert DataFeed is not None, f"Failed to import data feed for {data_source}, make sure it has been installed using e.g. `pip install pfeed[{data_source.value.lower()}]`"
    return DataFeed


def get_market_feed(
    data_source: tDATA_SOURCE, 
    data_tool: tDATA_TOOL='polars', 
    pipeline_mode: bool=False,
    use_ray: bool=True,
    use_prefect: bool=False,
    use_bytewax: bool=False,
    use_deltalake: bool=False,
) -> MarketFeed:
    from pfeed.feeds.market_feed import MarketFeed
    DataFeed = get_Feed(data_source)
    feed = DataFeed(
        data_tool=data_tool, 
        pipeline_mode=pipeline_mode, 
        use_ray=use_ray, 
        use_prefect=use_prefect, 
        use_bytewax=use_bytewax, 
        use_deltalake=use_deltalake,
    )
    if not isinstance(feed, MarketFeed):
        if hasattr(feed, 'market'):
            feed = feed.market
        else:
            raise ValueError(f"Data feed {feed} has no market feed")
    return feed