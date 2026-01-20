from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import BaseDataHandler

from abc import ABC

from pydantic import BaseModel, ConfigDict

from pfeed.enums import DataCategory


class BaseMetadataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

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

    data_origin: str = ""
    data_category: DataCategory

    def to_metadata(self, **fields) -> BaseMetadataModel:
        """Convert the data model to a metadata model.

        Args:
            **fields: Internal use only. Used to pass metadata fields up the 
                inheritance chain. Do not pass arguments directly when calling 
                this method.
        
        Returns:
            A metadata model instance containing the relevant metadata fields.
        """
        MetadataModel = self.metadata_class
        return MetadataModel(
            data_origin=self.data_origin,
            data_category=self.data_category,
            **fields,
        )
