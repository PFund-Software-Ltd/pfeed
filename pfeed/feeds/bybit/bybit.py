from pfeed.typing.literals import tDATA_TOOL
from pfeed.feeds.bybit.market_feed import BybitMarketFeed


class Bybit:
    def __init__(
        self,
        data_tool: tDATA_TOOL='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_bytewax: bool=False,
        use_deltalake: bool=False,
    ):
        params = {k: v for k, v in locals().items() if k != 'self'}
        self.market = BybitMarketFeed(**params)
