from __future__ import annotations

from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from pfeed.market.data_handler import MarketDataHandler
    from pfeed.market.data_model import MarketDataModel

from pfund_blocks.enums.env import Environment
from pydantic import model_validator

from pfeed.base.requests.replay_request import BaseReplayRequest
from pfeed.market.requests.base_request import MarketFeedBaseRequest


class MarketFeedReplayRequest(BaseReplayRequest, MarketFeedBaseRequest):
    env: Environment = Environment.BACKTEST

    # NOTE: not narrowed with Literal[Environment.BACKTEST], a subclass can't narrow a field's type
    @model_validator(mode="after")
    def _check_env(self) -> Self:
        # only BACKTEST data is replayed, i.e. data stored by download()
        if self.env != Environment.BACKTEST:
            raise ValueError(f"only BACKTEST data can be replayed, got {self.env}")
        return self

    def create_data_resolution_handler(
        self, data_model: MarketDataModel | None = None
    ) -> MarketDataHandler:
        """Handler reading this request's data from storage at data_resolution, see MarketFeedRetrieveRequest.create_data_resolution_handler().

        Args:
            data_model: date range to read, e.g. one date of the replay.
                Defaults to the request's full range.
        """
        from pfeed.enums import DataCategory

        data_model = (data_model or self.to_data_model()).model_copy(
            update={"resolution": self.data_resolution}
        )
        return data_model.DataHandler(
            data_model=data_model,
            io=self.io_for_replay,
            data_layer=self.data_layer,
            data_domain=str(DataCategory.MARKET_DATA),
            # NOTE: replayed data is always BACKTEST, i.e. stored by download(), never by a stream
            is_streaming=False,
        )
