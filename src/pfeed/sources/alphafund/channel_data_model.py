import time
from typing import ClassVar, Literal, Self
from uuid import UUID

from pydantic import UUID4, UUID5, Field, field_validator, model_validator

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
            'ON "channels" ("fund_id")',
        ),
    }
    insert_sql: ClassVar[dict[IOFormat, str]] = {
        IOFormat.SQLITE: """
            ON CONFLICT ("channel_id") DO UPDATE SET
                "channel_name" = excluded."channel_name",
                "user_ids" = excluded."user_ids",
                "agent_ids" = excluded."agent_ids",
                "is_archived" = excluded."is_archived"
        """,
    }

    fund_id: UUID5 | None = None
    channel_name: str | None = None
    channel_id: UUID5 | None = None
    channel_type: Literal["direct_message", "group_message"] = "direct_message"
    user_ids: list[UUID4] = Field(default_factory=list)
    agent_ids: list[UUID5] = Field(default_factory=list)
    created_at: float = Field(default_factory=time.time)
    is_archived: bool = False

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {column_name: False for column_name in cls.column_names()}

    @field_validator("user_ids", "agent_ids")
    @classmethod
    def normalize_member_ids(cls, member_ids: list[UUID]) -> list[UUID]:
        if len(member_ids) != len(set(member_ids)):
            raise ValueError("member IDs must not contain duplicates")
        return sorted(member_ids, key=str)

    @property
    def member_ids(self) -> list[UUID]:
        return [*self.user_ids, *self.agent_ids]

    @model_validator(mode="after")
    def validate_direct_member_count(self) -> Self:
        is_persistable = (
            self.fund_id is not None
            and self.channel_id is not None
            and self.channel_name is not None
        )
        if is_persistable and self.channel_type == "direct_message":
            if len(self.member_ids) != 2:
                raise ValueError(
                    "direct-message channels must have exactly two members"
                )
        return self
