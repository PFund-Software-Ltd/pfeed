from pfeed.data_client import DataClient
from pfeed.enums import DataCategory
from pfeed.sources.alphafund.fund_feed import AlphaFundFeed
from pfeed.sources.alphafund.mixin import AlphaFundMixin


class AlphaFund(AlphaFundMixin, DataClient):
    fund_feed: AlphaFundFeed
    # TODO: move chat into AgentFeed, remove ChatFeed
    # agent_feed: AlphaFundAgentFeed

    def _create_feeds(self):
        self.fund_feed = AlphaFundFeed(
            pipeline_mode=self._pipeline_mode,
            num_workers=(
                self._num_workers.get(DataCategory.FUND_DATA, None)
                if isinstance(self._num_workers, dict)
                else self._num_workers
            ),
        )
        # self.agent_feed = AlphaFundAgentFeed(
        #     pipeline_mode=self._pipeline_mode,
        #     num_workers=(
        #         self._num_workers.get(DataCategory.AGENT_DATA, None)
        #         if isinstance(self._num_workers, dict)
        #         else self._num_workers
        #     ),
        # )
