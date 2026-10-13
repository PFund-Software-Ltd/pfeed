from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Self

if TYPE_CHECKING:
    from pfeed.base.data_handler import BaseDataHandler

from pydantic import BaseModel, ConfigDict, model_validator


class BaseDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    DataHandler: ClassVar[type[BaseDataHandler]]

    data_source: str
    data_origin: str = ""

    @model_validator(mode="after")
    def _default_data_origin(self) -> Self:
        if not self.data_origin:
            self.data_origin = self.data_source
        return self

    def is_data_origin_effective(self) -> bool:
        """
        A data_origin is not effective if it is the same as the source name.
        """
        return self.data_origin != self.data_source

    def __str__(self) -> str:
        if self.is_data_origin_effective():
            return f"{self.data_source}:{self.data_origin}"
        else:
            return f"{self.data_source}"
