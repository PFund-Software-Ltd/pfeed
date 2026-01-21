from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfeed.enums import DataSource, DataCategory

from abc import ABC

class BaseSource(ABC):
    name: ClassVar[DataSource]

    def __init__(self, data_categories: list[DataCategory]):
        self.data_categories = data_categories
        
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        assert hasattr(cls, 'name'), f'{cls.__name__} must have a name attribute'
        assert isinstance(cls.name, DataSource), f'{cls.__name__} name must be a DataSource enum value'
