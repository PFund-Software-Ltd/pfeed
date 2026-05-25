from typing import ClassVar

from pfeed.enums import DataCategory, DataSource
from pfeed.sources.base_source import BaseSource


class PFundSource(BaseSource):
    NAME: ClassVar[DataSource] = DataSource.PFUND

    def get_data_categories(self) -> list[DataCategory]:
        return [DataCategory.ENGINE_DATA, DataCategory.COMPONENT_DATA]
