from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from enum import StrEnum
    from pfeed.enums import DataSource, DataCategory

from abc import ABC

class BaseSource(ABC):
    name: ClassVar[DataSource | StrEnum]

    def __init__(self, data_categories: list[DataCategory | StrEnum]):
        self.data_categories: list[DataCategory | StrEnum] = data_categories
