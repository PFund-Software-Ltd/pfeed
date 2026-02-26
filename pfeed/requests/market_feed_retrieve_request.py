from typing import Literal, Any

from pydantic import Field, field_validator

from pfund_kit.style import RichColor, TextStyle, cprint
from pfund.datas.resolution import Resolution
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    dataflow_per_date: bool = Field(description='Whether to create a dataflow for each date')
    data_resolution: Resolution | str = Field(
        description="Resolution of the raw data to retrieve from storage before resampling (if any) to target_resolution"
    )
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve

    @field_validator("data_resolution", mode="before")
    @classmethod
    def create_data_resolution(cls, v: Resolution | str) -> Resolution:
        if isinstance(v, str):
            return Resolution(v)
        return v

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.storage_config and self.storage_config.data_layer != DataLayer.RAW and self.clean_data:
            cprint(
                f'"clean_data" parameter is ignored when data layer is not RAW (got data layer={self.storage_config.data_layer})', 
                style=TextStyle.BOLD + RichColor.YELLOW
            )
            self.clean_data = False