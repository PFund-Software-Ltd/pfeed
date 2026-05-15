# pyright: reportUnusedParameter=false, reportUnknownParameterType=false
from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfeed.enums import DataSource

from abc import ABC, abstractmethod

from pfeed.enums import DataCategory

class BaseSource(ABC):
    NAME: ClassVar[DataSource]

    @property
    def name(self) -> str:
        return self.NAME

    @abstractmethod
    def get_data_categories(self) -> list[DataCategory]:
        pass
