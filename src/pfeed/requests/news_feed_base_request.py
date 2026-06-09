from pfund.entities.products.product_base import BaseProduct
from pfund.enums.env import Environment
from pydantic import field_validator

from pfeed.requests.time_based_feed_base_request import TimeBasedFeedBaseRequest


class NewsFeedBaseRequest(TimeBasedFeedBaseRequest):
    env: Environment
    product: BaseProduct | None = None

    @field_validator("env", mode="before")
    @classmethod
    def _validate_env(cls, v):
        if isinstance(v, str):
            return Environment[v.upper()]
        return v
