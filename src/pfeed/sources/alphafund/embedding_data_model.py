from typing import ClassVar, Literal

import re
import time
import hashlib
import math
from uuid import uuid4

import polars as pl
from pydantic import UUID4, Field, PrivateAttr

from pfeed.data_models.base_table_data_model import BaseTableDataModel
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundEmbeddingDataModel(BaseTableDataModel):
    """One embedded window of consecutive messages from a chat.

    Each embedding model gets its own table, because a vector column has a
    fixed width and vectors from different models are not comparable anyway.
    Rows are immutable: a window is never edited, only re-embedded. Uniqueness
    is on (chat_id, start_message_seq) within a model's table.
    """

    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "embedding_id"
    table_name_prefix: ClassVar[str] = "embeddings"

    # CRUD operations, no update and no delete
    _op: Literal["create", "read"] = PrivateAttr(init=False)
    created_at: float | None = None

    embedding_id: UUID4 | None = None
    fund_id: UUID4 | None = Field(
        default=None,
        description="Denormalized from the chat so every query can scope to a fund without a join.",
    )
    chat_id: UUID4 | None = None
    start_message_seq: int | None = Field(
        default=None,
        description="message_seq of the first message in the window, inclusive.",
    )
    end_message_seq: int | None = Field(
        default=None,
        description="message_seq of the last message in the window, inclusive.",
    )
    text: str | None = Field(
        default=None,
        description="The concatenated window text; kept for full-text search and for showing hits.",
    )
    vector: list[float] | None = None
    embedding_model: str | None = Field(
        default=None,
        description="'provider::model' reference that produced the vector.",
    )
    dimension: int | None = None

    @property
    def table_name(self) -> str:  # pyright: ignore[reportIncompatibleVariableOverride]
        if self.embedding_model is None:
            raise ValueError(
                "embedding_model must be set to locate the embeddings table"
            )
        slug = re.sub(r"[^a-z0-9]+", "_", self.embedding_model.lower()).strip("_")[:80]
        digest = hashlib.sha256(self.embedding_model.encode()).hexdigest()
        return f"{self.table_name_prefix}__{slug}__{digest}"

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {column_name: False for column_name in cls.column_names()}

    def polars_schema(self) -> dict[str, pl.DataType]:  # pyright: ignore[reportIncompatibleMethodOverride]
        schema = super().polars_schema()
        if self.dimension is not None:
            schema["vector"] = pl.Array(pl.Float32(), self.dimension)
        else:
            schema["vector"] = pl.List(pl.Float32())
        return schema

    def to_frame(self) -> pl.DataFrame:
        record = self.model_dump(mode="json", include=set(self.column_names()))
        return pl.DataFrame([record], schema=self.polars_schema())

    @property
    def op(self) -> Literal["create", "read"]:
        return self._op

    @op.setter
    def op(self, value: Literal["create", "read"]) -> None:
        self._op = value
        if self.fund_id is None or self.embedding_model is None:
            raise ValueError(
                "fund_id and embedding_model must be provided for any operation"
            )
        if self._op == "create":
            self._validate_create()
            self.embedding_id = uuid4()
            self.created_at = time.time()

    def _validate_create(self) -> None:
        if self.embedding_id is not None:
            raise ValueError("embedding_id must be None for create operation")
        required = (
            "fund_id",
            "chat_id",
            "start_message_seq",
            "end_message_seq",
            "text",
            "vector",
            "embedding_model",
            "dimension",
        )
        missing = [name for name in required if getattr(self, name) is None]
        if missing:
            raise ValueError(
                f"{', '.join(missing)} must be provided for create operation"
            )
        assert self.start_message_seq is not None and self.end_message_seq is not None
        assert self.vector is not None and self.dimension is not None
        if self.dimension <= 0:
            raise ValueError("dimension must be positive")
        if not all(math.isfinite(value) for value in self.vector) or not any(
            self.vector
        ):
            raise ValueError("cosine embeddings must be finite, nonzero vectors")
        if self.start_message_seq < 0 or self.end_message_seq < self.start_message_seq:
            raise ValueError(
                "window must satisfy 0 <= start_message_seq <= end_message_seq"
            )
        if len(self.vector) != self.dimension:
            raise ValueError(
                f"vector has {len(self.vector)} values but dimension is {self.dimension}"
            )
