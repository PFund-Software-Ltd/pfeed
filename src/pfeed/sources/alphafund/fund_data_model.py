from typing import ClassVar

from pydantic import UUID4, UUID5

from pfeed.data_models.base_sql_data_model import BaseSQLDataModel
from pfeed.enums import IOFormat
from pfeed.sources.alphafund.data_handler import AlphaFundDataHandler


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

    user_id: UUID4 | None = None
    fund_name: str | None = None
    fund_id: UUID5 | None = None

    @classmethod
    def column_nullability(cls) -> dict[str, bool]:
        return {column_name: False for column_name in cls.column_names()}
