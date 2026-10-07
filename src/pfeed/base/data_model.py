from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from pfeed.base.data_handler import BaseDataHandler

from pydantic import BaseModel, ConfigDict


class BaseDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    DataHandler: ClassVar[type[BaseDataHandler]]

    data_source: str
    data_origin: str = ""

    def model_post_init(self, __context: Any) -> None:
        if not self.data_origin:
            self.data_origin = self.data_source

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
