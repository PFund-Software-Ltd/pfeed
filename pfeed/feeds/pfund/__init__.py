from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import tENVIRONMENT
    from pfeed.typing import tDATA_TOOL


def PFund(
    env: tENVIRONMENT,
    data_tool: tDATA_TOOL='polars', 
    pipeline_mode: bool=False,
    use_ray: bool=True,
    use_prefect: bool=False,
    use_bytewax: bool=False,
    use_deltalake: bool=False,
):
    from pfund.enums import Environment
    from pfeed.feeds.pfund.backtesting_feed import BacktestingFeed
    from pfeed.feeds.pfund.trading_feed import TradingFeed
    params = {k: v for k, v in locals().items() if k not in ['self', 'env']}
    env = Environment[env.upper()]
    if env == Environment.BACKTEST:
        return BacktestingFeed(**params)
    else:
        return TradingFeed(**params)
