from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Any
if TYPE_CHECKING:
    from pfeed.data_handlers.base_data_handler import BaseDataHandler

from pydantic import BaseModel, ConfigDict

from pfeed.enums import DataSource
from pfeed.sources.data_provider_source import DataProviderSource


class BaseMetadataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    data_source: DataSource
    data_origin: str = ""


class BaseDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    
    data_handler_class: ClassVar[type[BaseDataHandler]]
    metadata_class: ClassVar[type[BaseMetadataModel]]

    data_source: DataProviderSource
    data_origin: str

    def model_post_init(self, __context: Any) -> None:
        if not self.data_origin:
            self.data_origin = self.data_source.name

    def is_data_origin_effective(self) -> bool:
        """
        A data_origin is not effective if it is the same as the source name.
        """
        return self.data_origin != self.data_source.name

    def __str__(self) -> str:
        if self.is_data_origin_effective():
            return f"{self.data_source.name}:{self.data_origin}"
        else:
            return f"{self.data_source.name}"
    
    def to_metadata(self, **fields: Any) -> BaseMetadataModel:
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
            data_source=self.data_source.name,
            data_origin=self.data_origin,
            **fields,
        )
