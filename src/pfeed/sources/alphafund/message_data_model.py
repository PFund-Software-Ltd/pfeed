from __future__ import annotations

import time
from typing import ClassVar, Literal

from pydantic import UUID4, UUID5, Field

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.enums import IOFormat
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundMessageDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "message_id"
    table_name: ClassVar[str] = "messages"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("message_id"),
        UNIQUE ("chat_id", "seq"),
        FOREIGN KEY ("chat_id") REFERENCES "chats" ("chat_id")
            ON DELETE CASCADE
    """
    insert_sql: ClassVar[dict[IOFormat, str]] = {
        IOFormat.SQLITE: """
            ON CONFLICT ("message_id") DO UPDATE SET
                "content" = excluded."content",
                "edited_at" = excluded."edited_at",
                "is_deleted" = excluded."is_deleted"
        """,
    }

    chat_id: UUID4
    # The fields below are None only while retrieving a chat's history.
    message_id: UUID4 | None = None
    # Per-chat monotonic ordering. uuid4 + a float clock is not a sort key:
    # ids are unordered and two messages can share a timestamp.
    seq: int | None = None
    author_id: UUID4 | UUID5 | None = Field(
        default=None,
        description="The user or agent that wrote the message; role alone does not say who.",
    )
    role: Literal["user", "agent"] = "user"
    content: str = ""
    created_at: float = Field(default_factory=time.time)
    edited_at: float | None = None
    is_deleted: bool = False

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {
            **super().column_nullability(),
            "message_id": False,
            "seq": False,
            "author_id": False,
        }
