from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfeed.enums import DataSource, DataCategory

from abc import ABC

class BaseSource(ABC):
    name: ClassVar[DataSource]

    def __init__(self, data_categories: list[DataCategory]):
        self.data_categories = data_categories
        