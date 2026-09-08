from uuid import UUID

from pfeed.data_client import DataClient
from pfeed.enums import DataCategory
from pfeed.sources.alphafund.agent_feed import AlphaFundAgentFeed
from pfeed.sources.alphafund.chat_feed import AlphaFundChatFeed
from pfeed.sources.alphafund.fund_feed import AlphaFundFeed
from pfeed.sources.alphafund.mixin import AlphaFundMixin


class AlphaFund(AlphaFundMixin, DataClient):
    fund_feed: AlphaFundFeed
    agent_feed: AlphaFundAgentFeed
    chat_feed: AlphaFundChatFeed

    def __init__(
        self,
        pipeline_mode: bool = False,
        num_workers: int | dict[DataCategory | str, int] | None = None,
        *,
        fund_id: UUID | str | None = None,
    ):
        """Bind entity feeds to a fund; an unbound client can use the fund registry."""
        self._fund_id = UUID(str(fund_id)) if fund_id is not None else None
        super().__init__(pipeline_mode=pipeline_mode, num_workers=num_workers)

    @property
    def fund_id(self) -> UUID | None:
        return self._fund_id

    def _create_feeds(self):
        self.fund_feed = AlphaFundFeed(
            pipeline_mode=self._pipeline_mode,
            num_workers=(
                self._num_workers.get(DataCategory.FUND_DATA, None)
                if isinstance(self._num_workers, dict)
                else self._num_workers
            ),
        )
        self.agent_feed = AlphaFundAgentFeed(
            fund_id=self._fund_id,
            pipeline_mode=self._pipeline_mode,
            num_workers=(
                self._num_workers.get(DataCategory.AGENT_DATA, None)
                if isinstance(self._num_workers, dict)
                else self._num_workers
            ),
        )
        self.chat_feed = AlphaFundChatFeed(
            fund_id=self._fund_id,
            pipeline_mode=self._pipeline_mode,
            num_workers=(
                self._num_workers.get(DataCategory.CHAT_DATA, None)
                if isinstance(self._num_workers, dict)
                else self._num_workers
            ),
        )
        self._feeds = [self.fund_feed, self.agent_feed, self.chat_feed]
