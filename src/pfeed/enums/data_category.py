from enum import StrEnum


class AlphaFundDataCategory(StrEnum):
    FUND_DATA = "FUND_DATA"
    AGENT_DATA = "AGENT_DATA"


class PFundDataCategory(StrEnum):
    ENGINE_DATA = "ENGINE_DATA"
    COMPONENT_DATA = "COMPONENT_DATA"


class DataCategory(StrEnum):
    MARKET_DATA = "MARKET_DATA"
    # NEWS_DATA = "NEWS_DATA"
    ENGINE_DATA = PFundDataCategory.ENGINE_DATA
    COMPONENT_DATA = PFundDataCategory.COMPONENT_DATA
    FUND_DATA = AlphaFundDataCategory.FUND_DATA
    AGENT_DATA = AlphaFundDataCategory.AGENT_DATA

    @property
    def feed_name(self) -> str:
        # e.g. DataCategory.MARKET_DATA -> market_feed
        return self.lower().replace("_data", "_feed")
