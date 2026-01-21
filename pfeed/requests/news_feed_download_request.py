from pydantic import field_validator

from pfund.enums import Environment
from pfund.products.product_base import BaseProduct
from pfeed.requests.time_based_feed_download_request import TimeBasedFeedDownloadRequest


class NewsFeedDownloadRequest(TimeBasedFeedDownloadRequest):
    env: Environment = Environment.LIVE
    product: BaseProduct | None = None

    @field_validator('env', mode='before')
    @classmethod
    def create_env(cls, v):
        if isinstance(v, str):
            return Environment[v.upper()]
        return v
    