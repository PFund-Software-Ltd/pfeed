from typing import Any, Literal

from pydantic import Field

from pfund.datas.resolution import Resolution
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(description='Whether to create a dataflow for each date')
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve
    data_resolution: Resolution | str | None = Field(
        default=None,
        description="Resolution of the data extracted from source before being resampled (if any) to target_resolution"
    )

    def model_post_init(self, __context: Any) -> None:
        from pfund_kit.style import RichColor, TextStyle, cprint
        super().model_post_init(__context)
        storage_config = self.storage_config
        if storage_config:
            is_raw_data = storage_config.data_layer == DataLayer.RAW
            # if it's not raw data, there's nothing to clean, clean_data is always False
            if not is_raw_data and self.clean_data:
                cprint(
                    f'"clean_data" parameter is ignored when data layer is not RAW (got data layer={storage_config.data_layer})',
                    style=TextStyle.BOLD + RichColor.YELLOW
                )
                self.clean_data = False
