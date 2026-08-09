from pfeed.data_client import DataClient
from pfeed.sources.alphafund.fund_feed import AlphaFundFeed
from pfeed.sources.alphafund.mixin import AlphaFundMixin


class AlphaFund(AlphaFundMixin, DataClient):
    fund_feed: AlphaFundFeed
    # TODO: move chat into AgentFeed, remove ChatFeed
    # agent_feed: AlphaFundAgentFeed

    def _create_feeds(self):
        self.fund_feed = AlphaFundFeed()
        # self.agent_feed = AlphaFundAgentFeed()
