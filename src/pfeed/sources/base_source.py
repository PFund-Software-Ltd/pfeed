# pyright: reportUnusedParameter=false, reportUnknownParameterType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from pfeed.enums import DataSource

from abc import ABC, abstractmethod

from pfeed.enums import DataCategory


class BaseSource(ABC):
    name: ClassVar[DataSource]

    def __init__(self):
        self._batch_api: Any | None = None
        self._stream_api: Any | None = None

    @abstractmethod
    def get_data_categories(self) -> list[DataCategory]:
        pass

    def get_batch_api(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(f"{self.name} does not support getting batch API")

    def get_stream_api(self, *args: Any, **kwargs: Any):
        raise NotImplementedError(f"{self.name} does not support getting stream API")
