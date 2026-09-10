from typing import Any, Literal

from pydantic import UUID4, BaseModel, Field

from pfeed.enums import ExtractType
from pfeed.requests.base_request import BaseRequest
from pfeed.storages.storage_config import StorageConfig
from pfeed.io.io_config import IOConfig


class AlphaFundChatFeedBaseDownloadRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.download
    storage_config: StorageConfig  # pyright: ignore[reportGeneralTypeIssues]
    io_config: IOConfig  # pyright: ignore[reportGeneralTypeIssues]
    # None means "leave as is": an update only writes the flags it was given.
    is_deleted: bool | None = None
    is_archived: bool | None = None


class AlphaFundChatFeedChannelDownloadRequest(AlphaFundChatFeedBaseDownloadRequest):
    fund_id: UUID4
    channel_name: str
    channel_id: UUID4 | None = None
    channel_type: Literal["direct_message", "group_chat"]
    user_ids: list[UUID4]
    agent_ids: list[UUID4]


class AlphaFundChatFeedChatDownloadRequest(AlphaFundChatFeedBaseDownloadRequest):
    channel_id: UUID4
    chat_name: str
    chat_id: UUID4 | None = None
    is_main: bool
    parent_message_id: UUID4 | None


class AlphaFundChatFeedMessageDownloadRequest(AlphaFundChatFeedBaseDownloadRequest):
    chat_id: UUID4
    content: str
    message_seq: int
    author_id: UUID4 = Field(description="user id or agent id")
    author_role: Literal["user", "agent", "system"]
    message_type: Literal["text", "compaction"] = "text"
    start_message_id: UUID4 | None = None
    end_message_id: UUID4 | None = None
    stop_reason: Literal["end_turn", "max_tokens", "max_turn_requests", "refusal", "cancelled"] | None = None
    cancellation_reason: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    message_id: UUID4 | None = None


class AlphaFundEmbeddingWindow(BaseModel):
    start_message_seq: int
    end_message_seq: int
    text: str
    vector: list[float]


class AlphaFundChatFeedEmbeddingDownloadRequest(BaseRequest):
    """One batch of embedded windows from a single chat under one model."""

    extract_type: ExtractType = ExtractType.download
    storage_config: StorageConfig  # pyright: ignore[reportGeneralTypeIssues]
    io_config: IOConfig  # pyright: ignore[reportGeneralTypeIssues]

    fund_id: UUID4
    chat_id: UUID4
    embedding_model: str = Field(description="'provider::model' reference")
    dimension: int
    windows: list[AlphaFundEmbeddingWindow] = Field(min_length=1)
