from __future__ import annotations
from typing import TYPE_CHECKING, overload, Literal
if TYPE_CHECKING:
    from pfeed.typing import tDataSource, tDataCategory
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.feeds.market_feed import MarketFeed

from pfeed.enums import DataCategory


def create_market_feed(
    data_source: tDataSource, 
    pipeline_mode: bool=False,
    **ray_kwargs,
) -> MarketFeed:
    params = {k: v for k, v in locals().items()}
    return create_feed(**params, data_category=DataCategory.MARKET_DATA, **ray_kwargs)


# EXTEND: add more @overload for different data sources and categories
@overload
def create_feed(
    data_source: tDataSource,
    data_category: Literal['MARKET_DATA'],
    pipeline_mode: bool = False,
    **ray_kwargs,
) -> MarketFeed: ...


def create_feed(
    data_source: tDataSource,
    data_category: tDataCategory,
    pipeline_mode: bool=False,
    **ray_kwargs,
) -> BaseFeed:
    import importlib
    from pfund_kit.utils.text import to_camel_case
    data_category = DataCategory[data_category.upper()]
    try:
        Feed: type[BaseFeed] = getattr(
            importlib.import_module(f'pfeed.sources.{data_source.lower()}.{data_category.feed_name}'),
            f'{to_camel_case(data_source)}{to_camel_case(data_category.feed_name)}'
        )
    except Exception:
        raise ValueError(f"{data_source} has no feed for {data_category}")
    feed: BaseFeed = Feed(pipeline_mode=pipeline_mode, **ray_kwargs)
    return feed