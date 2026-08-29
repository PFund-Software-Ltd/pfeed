from typing import ClassVar

from pydantic import UUID5

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.enums import IOFormat
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


class AlphaFundAgentDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    table_name: ClassVar[str] = "agents"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("agent_id"),
        UNIQUE ("fund_id", "agent_name")
    """
    insert_sql: ClassVar[dict[IOFormat, str]] = {
        IOFormat.SQLITE: """
            ON CONFLICT ("fund_id", "agent_name") DO NOTHING
        """,
    }

    fund_id: UUID5 | None = None
    agent_name: str | None = None
    agent_id: UUID5

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {column_name: False for column_name in cls.column_names()}
