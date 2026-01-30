from __future__ import annotations
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from pfeed.sources.alphafund.message_data_model import MessageDataModel

from uuid import uuid4
import time

from pydantic import BaseModel, UUID4, Field, ConfigDict


class ChatDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    """Bounded conversation session within a Channel in AlphaFund.

    A Chat exists because:
    1. LLMs have context limits - can't send infinite history
    2. Threads are lightweight/disposable (vs creating a new channel)

    Two types:
    - Main chat (is_main=True): The "lobby" - default space when entering channel
    - Thread chat (is_main=False): Side conversations, like Slack threads

    Inherits agents from Channel by default; can override for thread-subset scenarios.
    """
    chat_id: UUID4 = Field(default_factory=uuid4)
    channel_id: UUID4  # Which channel this chat belongs to
    agent_ids: list[int] | None = None  # None = inherit from channel; set for thread-subset
    messages: list[MessageDataModel] = Field(default_factory=list)
    title: str | None = None
    is_main: bool = False  # True = lobby chat, False = thread/side chat
    created_at: float = Field(default_factory=time.time)
    is_archived: bool = False

    def create_message(
        self,
        content: str,
        sender_type: Literal["user", "agent"],
        sender_id: UUID4 | int,
    ) -> 'MessageDataModel':
        """Create a new message within this chat.

        Args:
            content: The message text
            sender_type: "user" or "agent"
            sender_id: UUID if user, int if agent

        Returns:
            New MessageDataModel instance linked to this chat
        """
        from pfeed.sources.alphafund.message_data_model import MessageDataModel
        message_id = len(self.messages) + 1  # Sequential within chat
        return MessageDataModel(
            chat_id=self.chat_id,
            message_id=message_id,
            sender_type=sender_type,
            sender_id=sender_id,
            content=content,
        )

    def get_active_messages(self) -> list['MessageDataModel']:
        """Get all non-deleted messages."""
        return [msg for msg in self.messages if not msg.is_deleted]

    def __str__(self) -> str:
        main_str = "[main]" if self.is_main else "[thread]"
        archived = " [archived]" if self.is_archived else ""
        title = self.title or "Untitled"
        return f"Chat({title}, {main_str}, messages={len(self.messages)}){archived}"
