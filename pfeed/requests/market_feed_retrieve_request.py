from typing import Literal, Any

from pydantic import Field

from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(description='Whether to create a dataflow for each date')
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve

    def model_post_init(self, __context: Any) -> None:
        from pfund_kit.style import RichColor, TextStyle, cprint
        super().model_post_init(__context)
        if self.storage_config and self.storage_config.data_layer != DataLayer.RAW and self.clean_data:
            cprint(
                f'"clean_data" parameter is ignored when data layer is not RAW (got data layer={self.storage_config.data_layer})', 
                style=TextStyle.BOLD + RichColor.YELLOW
            )
            self.clean_data = False
