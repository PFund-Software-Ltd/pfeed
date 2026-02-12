from typing import Literal, Any

from pfeed.requests.market_feed_base_request import MarketFeedBaseRequest
from pfeed.enums import ExtractType, DataLayer


class MarketFeedDownloadRequest(MarketFeedBaseRequest):
    extract_type: Literal[ExtractType.download] = ExtractType.download

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        if self.storage_config and self.storage_config.data_domain and self.storage_config.data_layer != DataLayer.CURATED:
            raise ValueError(f'Custom data_domain={self.storage_config.data_domain} is only allowed when data layer is CURATED, but got data_layer={self.storage_config.data_layer}')
