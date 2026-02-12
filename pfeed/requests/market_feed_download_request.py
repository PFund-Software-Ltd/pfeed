from typing import Literal, Any

from pydantic import Field

from pfund_kit.style import RichColor, TextStyle, cprint
from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.download] = ExtractType.download
    clean_raw_data: bool = Field(
        default=True,
        description="""
            Whether to clean raw data after download when storage_config is None.
            When storage_config is provided, this parameter is ignored — cleaning is determined by data_layer instead.
            If True, downloaded raw data will be cleaned using the default transformations (normalize, standardize columns, resample, etc.).
            If False, downloaded raw data will be returned as is.
        """
    )

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.storage_config and self.storage_config.data_domain and self.storage_config.data_layer != DataLayer.CURATED:
            raise ValueError(f'Custom data_domain={self.storage_config.data_domain} is only allowed when data layer is CURATED, but got data_layer={self.storage_config.data_layer}')
        if self.storage_config and self.storage_config.data_layer == DataLayer.RAW and self.target_resolution != self.data_resolution:
            raise ValueError(f'No raw data for {self.product.name} with such resolution {self.target_resolution}')
