from typing import ClassVar, Literal, Self

import time
from uuid import UUID, uuid4

from pydantic import UUID4, Field, field_validator, model_validator, PrivateAttr

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.enums import IOFormat
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundChannelDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "channel_id"
    table_name: ClassVar[str] = "channels"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("channel_id"),
        UNIQUE ("fund_id", "channel_name"),
        FOREIGN KEY ("fund_id") REFERENCES "funds" ("fund_id")
            ON DELETE CASCADE
    """
    index_sql: ClassVar[dict[IOFormat, tuple[str, ...]]] = {
        IOFormat.SQLITE: (
            'CREATE INDEX IF NOT EXISTS "idx_channels_fund_id" '
            + 'ON "channels" ("fund_id")',
        ),
    }
    # CRUD operations, no delete
    _op: Literal["create", "read", "update"] = PrivateAttr(init=False)
    created_at: float | None = None
    updated_at: float | None = None
    is_deleted: bool = False
    is_archived: bool = False

    fund_id: UUID4 | None = None
    channel_name: str | None = None
    channel_id: UUID4 | None = None
    channel_type: Literal["direct_message", "group_chat"] = "direct_message"
    user_ids: list[UUID4] = Field(default_factory=list)
    agent_ids: list[UUID4] = Field(default_factory=list)

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {
            **{column_name: False for column_name in cls.column_names()},
            "updated_at": True,
        }

    @field_validator("user_ids", "agent_ids")
    @classmethod
    def normalize_member_ids(cls, member_ids: list[UUID]) -> list[UUID]:
        if len(member_ids) != len(set(member_ids)):
            raise ValueError("member IDs must not contain duplicates")
        return sorted(member_ids, key=str)

    @property
    def member_ids(self) -> list[UUID]:
        return [*self.user_ids, *self.agent_ids]

    def _validate_direct_member_count(self) -> None:
        if self.channel_type == "direct_message" and len(self.member_ids) != 2:
            raise ValueError("direct-message channels must have exactly two members")

    @model_validator(mode="after")
    def validate_direct_member_count(self) -> Self:
        is_persistable = (
            self.fund_id is not None
            and self.channel_id is not None
            and self.channel_name is not None
        )
        if is_persistable:
            self._validate_direct_member_count()
        return self

    @property
    def op(self) -> Literal["create", "read", "update"]:
        return self._op

    @op.setter
    def op(self, value: Literal["create", "read", "update"]) -> None:
        self._op = value
        if self._op == "create":
            if self.channel_id is not None:
                raise ValueError("channel_id must be None for create operation")
            self.channel_id = uuid4()
            self.created_at = time.time()
            self._validate_direct_member_count()
        elif self._op == "update":
            self.updated_at = time.time()
