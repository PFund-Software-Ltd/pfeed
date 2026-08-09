from uuid import UUID

from pfeed.requests.base_request import BaseRequest


class AlphaFundFeedBaseRequest(BaseRequest):
    user_id: UUID
    fund_name: str
    fund_id: UUID | None = None
