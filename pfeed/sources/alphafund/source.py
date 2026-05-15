from typing import ClassVar

from pfeed.enums import DataSource, DataCategory
from pfeed.sources.base_source import BaseSource


class AlphaFundSource(BaseSource):
    NAME: ClassVar[DataSource] = DataSource.ALPHAFUND

    def get_data_categories(self) -> list[DataCategory]:
        return [DataCategory.CHAT_DATA]
