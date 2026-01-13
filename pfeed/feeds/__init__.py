from __future__ import annotations
from typing import TYPE_CHECKING, overload, Literal
if TYPE_CHECKING:
    from pfeed.typing import tDataSource, tDataTool, tDataCategory
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.feeds.market_feed import MarketFeed

from pfeed.enums import DataCategory


def create_market_feed(
    data_source: tDataSource, 
    data_tool: tDataTool='polars', 
    pipeline_mode: bool=False,
    use_ray: bool=True,
    use_prefect: bool=False,
    use_deltalake: bool=False,
) -> MarketFeed:
    params = {k: v for k, v in locals().items()}
    return create_feed(**params, data_category=DataCategory.MARKET_DATA)


# EXTEND: add more @overload for different data sources and categories
@overload
def create_feed(
    data_source: tDataSource,
    data_category: Literal['MARKET_DATA'],
    data_tool: tDataTool = 'polars',
    pipeline_mode: bool = False,
    use_ray: bool = True,
    use_prefect: bool = False,
    use_deltalake: bool = False,
) -> MarketFeed: ...


def create_feed(
    data_source: tDataSource,
    data_category: tDataCategory,
    data_tool: tDataTool='polars', 
    pipeline_mode: bool=False,
    use_ray: bool=True,
    use_prefect: bool=False,
    use_deltalake: bool=False,
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
    feed: BaseFeed = Feed(
        data_tool=data_tool, 
        pipeline_mode=pipeline_mode, 
        use_ray=use_ray, 
        use_prefect=use_prefect, 
        use_deltalake=use_deltalake,
    )
    return feed