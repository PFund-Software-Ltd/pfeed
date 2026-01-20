from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import BaseDataHandler

from abc import ABC

from pydantic import BaseModel, ConfigDict

from pfeed.enums import DataCategory


class BaseMetadataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid", frozen=True)

    data_category: DataCategory


class BaseDataModel(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    
    data_handler_class: ClassVar[type[BaseDataHandler]]
    metadata_class: ClassVar[type[BaseMetadataModel]]

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
            data_category=self.data_category,
            **fields,
        )
