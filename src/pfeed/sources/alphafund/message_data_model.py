from typing import ClassVar, Literal, Self

import time
from uuid import uuid4

from pydantic import UUID4, Field, model_validator, PrivateAttr

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundMessageDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "message_id"
    table_name: ClassVar[str] = "messages"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("message_id"),
        UNIQUE ("chat_id", "message_seq"),
        CHECK ("message_seq" >= 0),
        CHECK ("author_role" IN ('user', 'agent')),
        FOREIGN KEY ("chat_id") REFERENCES "chats" ("chat_id")
            ON DELETE CASCADE
    """
    # CRUD operations, no delete
    _op: Literal["create", "read", "update"] = PrivateAttr(init=False)
    created_at: float | None = None
    updated_at: float | None = None
    is_deleted: bool = False
    is_archived: bool = False

    chat_id: UUID4 | None = None
    content: str | None = None
    message_id: UUID4 | None = None
    message_seq: int | None = Field(
        default=None,
        description="The message's monotonically increasing position within its chat.",
    )
    author_id: UUID4 | None = Field(
        default=None,
        description="The user or agent that wrote the message; role alone does not say who.",
    )
    author_role: Literal["user", "agent"] = "user"

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {
            **{column_name: False for column_name in cls.column_names()},
            "updated_at": True,
        }

    @property
    def op(self) -> Literal["create", "read", "update"]:
        return self._op

    @op.setter
    def op(self, value: Literal["create", "read", "update"]) -> None:
        self._op = value
        if self._op == "create":
            if self.message_id is not None:
                raise ValueError("message_id must be None for create operation")
            self.message_id = uuid4()
            self.created_at = time.time()
        elif self._op == "update":
            self.updated_at = time.time()
