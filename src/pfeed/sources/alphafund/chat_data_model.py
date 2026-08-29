from __future__ import annotations

import time
from typing import ClassVar

from pydantic import UUID4, UUID5, Field, model_validator

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.enums import IOFormat
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundChatDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "chat_id"
    table_name: ClassVar[str] = "chats"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("chat_id"),
        UNIQUE ("channel_id", "parent_message_id"),
        CHECK (
            ("is_main" = 1 AND "parent_message_id" IS NULL)
            OR
            ("is_main" = 0 AND "parent_message_id" IS NOT NULL)
        ),
        FOREIGN KEY ("channel_id") REFERENCES "channels" ("channel_id")
            ON DELETE CASCADE
    """
    index_sql: ClassVar[dict[IOFormat, tuple[str, ...]]] = {
        IOFormat.SQLITE: (
            'CREATE UNIQUE INDEX IF NOT EXISTS "idx_chats_one_main_per_channel" '
            'ON "chats" ("channel_id") WHERE "is_main" = 1',
        ),
    }
    insert_sql: ClassVar[dict[IOFormat, str]] = {
        IOFormat.SQLITE: """
            ON CONFLICT ("chat_id") DO UPDATE SET
                "chat_name" = excluded."chat_name",
                "is_archived" = excluded."is_archived"
        """,
    }

    channel_id: UUID5
    chat_id: UUID4
    chat_name: str = Field(default="", description="The chat name used as its title.")
    # None identifies a lookup model containing only channel_id and chat_id.
    # Persisted chat rows must provide whether they are the channel's main chat.
    is_main: bool | None = None
    # Points at the lobby message this chat was opened from. A reference, not
    # ownership: the chat belongs to the channel either way. NULL for the lobby.
    # This cannot be an SQLite FK while tables are created on first write: chats
    # and messages would otherwise each require the other table to exist first.
    parent_message_id: UUID4 | None = None
    created_at: float = Field(default_factory=time.time)
    is_archived: bool = False

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {
            **super().column_nullability(),
            "chat_id": False,
            "is_main": False,
        }

    @model_validator(mode="after")
    def validate_chat_kind(self) -> AlphaFundChatDataModel:
        # A model without chat-kind metadata is a read filter.
        if self.is_main is None:
            return self
        if self.is_main != (self.parent_message_id is None):
            raise ValueError(
                "main chats must not have parent_message_id; "
                "thread chats must have parent_message_id"
            )
        return self
