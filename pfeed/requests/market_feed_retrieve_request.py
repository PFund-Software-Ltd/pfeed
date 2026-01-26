from typing import Literal

from pydantic import Field, model_validator, field_validator

from pfund_kit.style import RichColor, TextStyle, cprint
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedRetrieveRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.retrieve] = ExtractType.retrieve
    data_layer: DataLayer
    clean_raw_data: bool = Field(
        default=False,
        description="""
            Whether to clean raw data. 
            If True, raw data stored in data layer=RAW will be cleaned using the default transformations for download.
            If False, raw data stored in data layer=RAW will be loaded as is.
            If data_layer is not RAW, this parameter will not be used and a warning will be raised.
        """
    )

    @field_validator('data_layer', mode='before')
    @classmethod
    def create_data_layer(cls, v):
        if isinstance(v, str):
            return DataLayer[v.upper()]
        return v
    
    @model_validator(mode="after")
    def validate_clean_raw_data(self):
        if self.data_layer != DataLayer.RAW and self.clean_raw_data:
            cprint(
                f'"clean_raw_data" parameter is ignored when data layer is not RAW (got data layer={self.data_layer})', 
                style=TextStyle.BOLD + RichColor.YELLOW
            )
            self.clean_raw_data = False
        return self