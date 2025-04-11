from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.typing import tENVIRONMENT
    from pfund.enums import ComponentType
    from pfeed.typing import tDATA_TOOL

import datetime

from pfeed.sources._pfund_source import PFundSource
from pfeed.feeds.base_feed import BaseFeed
from pfeed.data_models.pfund_data_model import PFundDataModel


class PFundDataFeed(BaseFeed):
    @staticmethod
    def _create_data_source() -> PFundSource:
        return PFundSource()

    def create_data_model(
        self, 
        start_date: datetime.date | None=None,
        end_date: datetime.date | None=None,
        component_name: str | None=None,
        component_type: ComponentType | None=None,
    ) -> PFundDataModel:
        return PFundDataModel(
            ...
        )