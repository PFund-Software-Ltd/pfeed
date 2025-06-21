from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDataSource, tDataTool
    from pfeed.feeds.market_feed import MarketFeed


def get_market_feed(
    data_source: tDataSource, 
    data_tool: tDataTool='polars', 
    pipeline_mode: bool=False,
    use_ray: bool=True,
    use_prefect: bool=False,
    use_deltalake: bool=False,
) -> MarketFeed:
    from pfeed.enums import DataSource
    DataClient = DataSource[data_source.upper()].data_client
    data_client = DataClient(
        data_tool=data_tool, 
        pipeline_mode=pipeline_mode, 
        use_ray=use_ray, 
        use_prefect=use_prefect, 
        use_deltalake=use_deltalake,
    )
    if not hasattr(data_client, 'market_feed'):
        raise ValueError(f"Data client {data_client} has no market feed")
    return data_client.market_feed