from pydantic import UUID4, UUID5

from pfeed.requests.base_request import BaseRequest


class AlphaFundFeedBaseRequest(BaseRequest):
    user_id: UUID4 | None = None
    fund_name: str | None = None
    fund_id: UUID5
