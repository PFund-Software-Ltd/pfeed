from typing import ClassVar, Literal, Self

import time
from uuid import uuid4

from pydantic import UUID4, PrivateAttr, model_validator

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    identity_column: ClassVar[str] = "fund_id"
    table_name: ClassVar[str] = "funds"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("fund_id"),
        UNIQUE ("user_id", "fund_name")
    """
    # CRUD operations, no delete
    _op: Literal["create", "read", "update"] = PrivateAttr(init=False)
    created_at: float | None = None
    updated_at: float | None = None
    is_deleted: bool = False

    user_id: UUID4 | None = None
    fund_name: str | None = None
    fund_id: UUID4 | None = None

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
            if self.fund_id is not None:
                raise ValueError("fund_id must be None for create operation")
            self.fund_id = uuid4()
            self.created_at = time.time()
        elif self._op == "update":
            self.updated_at = time.time()
