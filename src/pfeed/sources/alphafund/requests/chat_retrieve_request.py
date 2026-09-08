from typing import Any

from pydantic import UUID4, Field, model_validator

from pfeed.enums import ExtractType
from pfeed.io.io_config import IOConfig
from pfeed.requests.base_request import BaseRequest
from pfeed.storages.storage_config import StorageConfig


class AlphaFundChatFeedRetrieveRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.retrieve

    fund_id: UUID4 | None = None
    channel_id: UUID4 | None = None
    chat_id: UUID4 | None = None

    storage_config_for_retrieval: StorageConfig
    io_config_for_retrieval: IOConfig


class AlphaFundChatFeedEmbeddingRetrieveRequest(BaseRequest):
    extract_type: ExtractType = ExtractType.retrieve

    fund_id: UUID4
    embedding_model: str = Field(
        description="'provider::model' reference; selects the table"
    )
    chat_id: UUID4 | None = Field(
        default=None, description="None means every chat in the fund"
    )
    columns: list[str] | None = Field(
        default=None,
        description="Projection; leave the vector column out when only bookkeeping columns are needed.",
    )

    storage_config_for_retrieval: StorageConfig
    io_config_for_retrieval: IOConfig


class AlphaFundChatFeedSearchRequest(AlphaFundChatFeedEmbeddingRetrieveRequest):
    query_vector: list[float] | None = None
    query_text: str | None = None
    limit: int = Field(default=10, gt=0)
    search_kwargs: dict[str, Any] = Field(
        default_factory=dict,
        description="Backend tuning knobs passed through to the IO, e.g. nprobes, refine_factor.",
    )

    @model_validator(mode="after")
    def validate_query(self):
        if self.query_vector is None and self.query_text is None:
            raise ValueError("search requires query_vector, query_text, or both")
        return self
