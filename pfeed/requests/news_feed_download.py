from pfund.enums import Environment
from pfund.products.product_base import BaseProduct
from pfeed.requests.time_based_feed_download import TimeBasedFeedDownloadRequest


class NewsFeedDownloadRequest(TimeBasedFeedDownloadRequest):
    env: Environment = Environment.LIVE
    product: BaseProduct | None = None