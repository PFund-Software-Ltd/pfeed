from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.sources.alphafund.chat_data_model import ChatDataModel

import time
from uuid import uuid4

from pydantic import BaseModel, UUID4, Field, ConfigDict, model_validator


class ChannelDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    channel_id: UUID4 = Field(default_factory=uuid4)
    owner_id: UUID4  # Channel creator with admin rights
    user_ids: list[UUID4] = Field(default_factory=list)  # All members (including owner)
    agent_ids: list[int] = Field(default_factory=list)
    chats: list[ChatDataModel] = Field(default_factory=list)
    name: str  # e.g., "Trading Room", "Trader DM"
    description: str = ''
    created_at: float = Field(default_factory=time.time)
    is_archived: bool = False

    @model_validator(mode='after')
    def _post_init(self) -> 'ChannelDataModel':
        """Post-initialization setup."""
        # Ensure owner is in user_ids
        if self.owner_id not in self.user_ids:
            self.user_ids.append(self.owner_id)
        # Ensure channel has at least one main chat
        if not self.chats:
            self.chats.append(self._create_main_chat())
        return self

    def _create_main_chat(self) -> 'ChatDataModel':
        """Create the main lobby chat. Internal use only."""
        from pfeed.sources.alphafund.chat_data_model import ChatDataModel
        return ChatDataModel(
            channel_id=self.channel_id,
            title=self.name,
            is_main=True,
        )

    def create_chat(
        self,
        title: str | None = None,
        agent_ids: list[int] | None = None,
    ) -> 'ChatDataModel':
        """Create a new thread/side chat within this channel.

        Note: Main chat is auto-created when channel is created.
        This method only creates thread chats (is_main=False).

        Args:
            title: Optional title for the chat (e.g., "Debug order #123")
            agent_ids: Override channel's agent_ids for this chat (e.g., thread subset)

        Returns:
            New ChatDataModel instance linked to this channel
        """
        from pfeed.sources.alphafund.chat_data_model import ChatDataModel
        return ChatDataModel(
            channel_id=self.channel_id,
            agent_ids=agent_ids,
            title=title,
            is_main=False,
        )

    def get_main_chat(self) -> 'ChatDataModel | None':
        """Get the main lobby chat."""
        for chat in self.chats:
            if chat.is_main:
                return chat
        return None

    def get_active_chats(self) -> list['ChatDataModel']:
        """Get all non-archived chats."""
        return [chat for chat in self.chats if not chat.is_archived]

    def __str__(self) -> str:
        archived = " [archived]" if self.is_archived else ""
        return f"Channel({self.name}, users={len(self.user_ids)}, agents={len(self.agent_ids)}, chats={len(self.chats)}){archived}"
