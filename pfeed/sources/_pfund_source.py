from enum import StrEnum

from pfeed.sources.base_source import BaseSource


# NOTE: only used as a workaround of the "data_source: BaseSource" requirement in e.g. BaseDataModel, BaseFeed, etc.
class PFundSource(BaseSource):
    def __init__(self):
        self.name = StrEnum('PFundSource', ['PFund']).PFund
