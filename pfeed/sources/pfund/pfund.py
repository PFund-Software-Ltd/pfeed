from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund._typing import tEnvironment
    from pfeed._typing import tDataTool
    from pfeed.sources.pfund.batch_api import BatchAPI
    from pfeed.sources.pfund.stream_api import StreamAPI

from pfund.enums import Environment
from pfeed.sources.pfund.source import PFundSource
from pfeed.sources.pfund.engine_feed import PFundEngineFeed


__all__ = ['PFund']


class PFund:
    def __init__(
        self,
        env: tEnvironment,
        # TODO
        # channels: PFundChannels | None=None,
        data_tool: tDataTool='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
    ):
        params = {k: v for k, v in locals().items() if k not in ['self', 'env']}
        self.env = Environment[env.upper()]
        self.data_source = PFundSource(env=env)
        self.engine_feed = PFundEngineFeed(env=env, data_source=self.data_source, **params)

    @property
    def name(self) -> str:
        return self.data_source.name
    
    @property
    def engine_data(self) -> PFundEngineFeed:
        return self.engine_feed
    engine = engine_data
    
    @property
    def batch_api(self) -> BatchAPI:
        return self.data_source.batch_api
    
    @property
    def stream_api(self) -> StreamAPI:
        return self.data_source.stream_api
    