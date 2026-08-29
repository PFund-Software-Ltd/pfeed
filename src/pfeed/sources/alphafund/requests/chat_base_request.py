from pydantic import UUID4, UUID5

from pfeed.requests.base_request import BaseRequest


class AlphaFundChatFeedBaseRequest(BaseRequest):
    fund_id: UUID5 | None = None
    channel_name: str | None = None
    channel_id: UUID5 | None = None
    chat_id: UUID4 | None = None
