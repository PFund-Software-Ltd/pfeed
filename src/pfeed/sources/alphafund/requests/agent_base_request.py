from pydantic import UUID5

from pfeed.requests.base_request import BaseRequest


class AlphaFundAgentFeedBaseRequest(BaseRequest):
    fund_id: UUID5 | None = None
    agent_name: str | None = None
    agent_id: UUID5
