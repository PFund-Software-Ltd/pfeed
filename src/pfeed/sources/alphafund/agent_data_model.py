import time
from typing import ClassVar, Literal
from uuid import uuid4

from pydantic import UUID4, PrivateAttr

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundAgentDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "agent_id"
    table_name: ClassVar[str] = "agents"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("agent_id"),
        UNIQUE ("fund_id", "agent_name"),
        FOREIGN KEY ("fund_id") REFERENCES "funds" ("fund_id")
            ON DELETE CASCADE
    """
    # CRUD operations, no delete
    _op: Literal["create", "read", "update"] = PrivateAttr(init=False)
    created_at: float | None = None
    updated_at: float | None = None
    is_deleted: bool = False

    fund_id: UUID4 | None = None
    agent_name: str | None = None
    agent_role: str | None = None
    agent_class: str | None = None
    agent_id: UUID4 | None = None

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
            if self.agent_id is not None:
                raise ValueError("agent_id must be None for create operation")
            self.agent_id = uuid4()
            self.created_at = time.time()
        elif self._op == "update":
            self.updated_at = time.time()
