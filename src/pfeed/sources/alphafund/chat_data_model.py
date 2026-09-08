from typing import ClassVar, Self, Literal

import time
from uuid import uuid4

from pydantic import UUID4, Field, model_validator, PrivateAttr

from pfeed.data_models.base_table_data_model import BaseTableDataModel
from pfeed.enums import IOFormat
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundChatDataModel(BaseTableDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "chat_id"
    table_name: ClassVar[str] = "chats"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("chat_id"),
        UNIQUE ("channel_id", "chat_name"),
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
            + 'ON "chats" ("channel_id") WHERE "is_main" = 1',
        ),
    }

    # CRUD operations, no delete
    _op: Literal["create", "read", "update"] = PrivateAttr(init=False)
    created_at: float | None = None
    updated_at: float | None = None
    is_deleted: bool = False
    is_archived: bool = False

    fund_id: UUID4 | None = None
    channel_id: UUID4 | None = None
    chat_name: str | None = Field(
        default=None, description="The chat name used as its title."
    )
    chat_id: UUID4 | None = None
    is_main: bool = Field(
        default=False,
        description="True if this is the main chat; False if this is a thread chat.",
    )
    parent_message_id: UUID4 | None = Field(
        default=None,
        description="The lobby message that started this thread; None for the main chat.",
    )

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {
            **{column_name: False for column_name in cls.column_names()},
            "updated_at": True,
            "parent_message_id": True,
        }

    def _validate_chat_kind(self):
        if self.is_main and self.parent_message_id is not None:
            raise ValueError("main chat must not have parent_message_id")

        if not self.is_main and self.parent_message_id is None:
            raise ValueError("thread chat must have parent_message_id")

    @property
    def op(self) -> Literal["create", "read", "update"]:
        return self._op

    @op.setter
    def op(self, value: Literal["create", "read", "update"]) -> None:
        self._op = value
        if self._op == "create":
            if self.chat_id is not None:
                raise ValueError("chat_id must be None for create operation")
            self.chat_id = uuid4()
            self.created_at = time.time()
        elif self._op == "update":
            self.updated_at = time.time()
        if self._op in {"create", "update"}:
            self._validate_chat_kind()
