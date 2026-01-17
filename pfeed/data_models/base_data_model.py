from __future__ import annotations
from typing import Any, TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import BaseDataHandler

from abc import ABC

from pydantic import BaseModel, ConfigDict

from pfeed.sources.base_source import BaseSource
from pfeed.enums import DataCategory, DataSource


class BaseMetadataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    data_source: DataSource
    data_origin: str
    data_category: DataCategory


class BaseDataModel(BaseModel, ABC):
    """
    Args:
        source: The source of the data.
        data_origin:
            A unique identifier for the data.
            If specified, it will be used to differentiate where the data is actually from.
                For example,
                    for Databento, Publisher (comprised of dataset and trading venue, e.g. DATASET_VENUE) is used to be the unique identifier.
                    This is because a product can be traded on multiple venues.
            If None, it means the data source is already a unique identifier.
            Default is None.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    
    data_handler_class: ClassVar[type[BaseDataHandler]]
    metadata_class: ClassVar[type[BaseMetadataModel]]

    data_source: BaseSource
    data_origin: str = ""
    data_category: DataCategory

    def model_post_init(self, __context: Any) -> None:
        if not self.data_origin:
            self.data_origin = self.data_source.name.value

    def is_data_origin_effective(self) -> bool:
        """
        A data_origin is not effective if it is the same as the source name.
        """
        return self.data_origin != self.data_source.name.value

    def __str__(self):
        if self.is_data_origin_effective():
            return f"{self.data_source.name.value}:{self.data_origin}"
        else:
            return f"{self.data_source.name.value}"

    def to_metadata(self) -> BaseMetadataModel:
        return BaseMetadataModel(
            data_source=self.data_source,
            data_origin=self.data_origin,
        )
