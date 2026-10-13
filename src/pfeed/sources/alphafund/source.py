from typing import ClassVar

from pfeed.enums import DataCategory
from pfeed.source import BaseSource


class AlphaFundSource(BaseSource):
    name: ClassVar[str] = "ALPHAFUND"

    def get_data_categories(self) -> list[DataCategory]:
        return [
            DataCategory.FUND_DATA,
            DataCategory.AGENT_DATA,
            DataCategory.CHAT_DATA,
        ]
