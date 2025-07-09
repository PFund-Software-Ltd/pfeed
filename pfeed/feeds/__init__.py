from __future__ import annotations
from typing import TYPE_CHECKING, overload, Literal
if TYPE_CHECKING:
    from pfund.typing import tEnvironment
    from pfeed.typing import tDataSource, tDataTool, tDataCategory
    from pfeed.feeds.base_feed import BaseFeed
    from pfeed.feeds.market_feed import MarketFeed

from pfeed.enums import DataCategory


def get_market_feed(
    data_source: tDataSource, 
    data_tool: tDataTool='polars', 
    pipeline_mode: bool=False,
    use_ray: bool=True,
    use_prefect: bool=False,
    use_deltalake: bool=False,
    env: tEnvironment='BACKTEST',
) -> MarketFeed:
    params = {k: v for k, v in locals().items()}
    return get_feed(**params, data_category=DataCategory.MARKET_DATA)


# EXTEND: add more @overload for different data sources and categories
@overload
def get_feed(
    data_source: tDataSource,
    data_category: Literal['MARKET_DATA'],
    data_tool: tDataTool = 'polars',
    pipeline_mode: bool = False,
    use_ray: bool = True,
    use_prefect: bool = False,
    use_deltalake: bool = False,
    env: tEnvironment = 'LIVE',
) -> MarketFeed: ...


def get_feed(
    data_source: tDataSource,
    data_category: tDataCategory,
    data_tool: tDataTool='polars', 
    pipeline_mode: bool=False,
    use_ray: bool=True,
    use_prefect: bool=False,
    use_deltalake: bool=False,
    env: tEnvironment='LIVE',
) -> BaseFeed:
    import importlib
    from pfeed.utils.utils import to_camel_case
    data_category = DataCategory[data_category.upper()]
    try:
        Feed: type[BaseFeed] = getattr(
            importlib.import_module(f'pfeed.sources.{data_source.lower()}.{data_category.feed_name}'),
            f'{to_camel_case(data_source)}{to_camel_case(data_category.feed_name)}'
        )
    except Exception:
        raise ValueError(f"{data_source} has no feed for {data_category}")
    feed: BaseFeed = Feed(
        env=env,
        data_source=data_source,
        data_tool=data_tool, 
        pipeline_mode=pipeline_mode, 
        use_ray=use_ray, 
        use_prefect=use_prefect, 
        use_deltalake=use_deltalake,
    )
    return feed