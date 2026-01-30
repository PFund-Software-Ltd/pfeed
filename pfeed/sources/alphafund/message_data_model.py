from typing import Literal
import time

from pydantic import BaseModel, UUID4, Field, ConfigDict


class MessageDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    """Individual message within a Chat in AlphaFund.

    A message has a sender and is sent to a chat (everyone in chat sees it).
    - message_id is sequential within a chat for ordering
    - sender_id is UUID (user) or int (agent) depending on role
    """
    chat_id: UUID4  # Which chat this message belongs to
    message_id: int  # Sequential within chat for ordering
    sender_type: Literal["user", "agent"]
    sender_id: UUID4 | int  # UUID if user, int if agent
    content: str
    created_at: float = Field(default_factory=time.time)
    edited_at: float | None = None
    is_deleted: bool = False  # Soft delete

    def __str__(self) -> str:
        deleted = " [deleted]" if self.is_deleted else ""
        preview = self.content[:50] + "..." if len(self.content) > 50 else self.content
        return f"Message(#{self.message_id}, {self.sender_type}, '{preview}'){deleted}"
