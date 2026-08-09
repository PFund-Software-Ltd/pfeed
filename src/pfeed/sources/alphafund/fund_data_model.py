from typing import ClassVar
from uuid import uuid4

from pydantic import UUID4, Field

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.enums import IOFormat
from pfeed.sources.alphafund.fund_data_handler import AlphaFundDataHandler


class AlphaFundDataModel(BaseSQLDataModel):
    DataHandler: ClassVar[type[AlphaFundDataHandler]] = AlphaFundDataHandler

    table_name: ClassVar[str] = "funds"
    table_sql: ClassVar[str] = """
        PRIMARY KEY ("fund_id"),
        UNIQUE ("user_id", "fund_name")
    """
    insert_sql: ClassVar[dict[IOFormat, str]] = {
        IOFormat.SQLITE: """
            ON CONFLICT ("user_id", "fund_name") DO NOTHING
        """,
    }

    user_id: UUID4
    fund_id: UUID4 = Field(default_factory=uuid4)
    fund_name: str = Field(default="AlphaFund")
