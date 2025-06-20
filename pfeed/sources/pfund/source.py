from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.enums import Environment

from enum import StrEnum

from pfeed.sources.base_source import BaseSource


class PFundSource(BaseSource):
    name = StrEnum('PFundSource', ['PFund']).PFund
    
    def __init__(self, env: Environment):
        from pfeed.sources.pfund.batch_api import BatchAPI
        from pfeed.sources.pfund.stream_api import StreamAPI
        super().__init__()
        self.batch_api = BatchAPI()
        self.stream_api = StreamAPI(env=env)

    # HACK: only to satisfy the BaseSource requirement
    def create_product(self, basis: str, symbol: str='', **specs):
        pass