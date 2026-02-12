from typing import Literal, Any

from pydantic import Field

from pfund_kit.style import RichColor, TextStyle, cprint
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve
    clean_raw_data: bool = Field(
        default=False,
        description="""
            Whether to clean raw data. 
            If True, raw data stored in data layer=RAW will be cleaned using the default transformations for download.
            If False, raw data stored in data layer=RAW will be loaded as is.
            If data_layer is not RAW, this parameter will not be used and a warning will be raised.
        """
    )

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.storage_config and self.storage_config.data_layer != DataLayer.RAW and self.clean_raw_data:
            cprint(
                f'"clean_raw_data" parameter is ignored when data layer is not RAW (got data layer={self.storage_config.data_layer})', 
                style=TextStyle.BOLD + RichColor.YELLOW
            )
            self.clean_raw_data = False