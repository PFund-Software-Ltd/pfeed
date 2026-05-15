from typing import ClassVar

from pfeed.sources.base_source import BaseSource
from pfeed.enums import DataSource, DataCategory


class PFundSource(BaseSource):
    NAME: ClassVar[DataSource] = DataSource.PFUND

    def get_data_categories(self) -> list[DataCategory]:
        return [DataCategory.ENGINE_DATA, DataCategory.COMPONENT_DATA]