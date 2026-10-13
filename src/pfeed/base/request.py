from __future__ import annotations

from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from pfeed.base.data_model import BaseDataModel

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pfeed.enums import DataLayer, ExtractType
from pfeed.io.base_io import BaseIO


class BaseRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    data_source: str
    data_origin: str = ""
    extract_type: ExtractType
    io: BaseIO | None = None
    data_layer: DataLayer = Field(
        default=DataLayer.CLEANED,
        description="""
            Data layer of the data: what is produced for download/stream (RAW or CLEANED),
            where it is read from for retrieve.
        """,
    )

    def to_data_model(self) -> BaseDataModel:
        raise NotImplementedError

    def __hash__(self) -> int:
        return hash(id(self))

    def __eq__(self, other: object) -> bool:
        return self is other

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def should_clean_data(self) -> bool:
        """Whether to clean raw data using the default transformations (normalize, standardize columns, resample, etc.)."""
        return self.data_layer != DataLayer.RAW

    def is_streaming(self) -> bool:
        return False

    def is_replaying(self) -> bool:
        return False

    @field_validator("data_source", mode="before")
    @classmethod
    def _validate_data_source(cls, value: str) -> str:
        from pfeed import registry
        return registry.get_entry_point(value).name.upper()

    @field_validator("data_layer", mode="before")
    @classmethod
    def _validate_data_layer(cls, value: DataLayer | str) -> DataLayer:
        if isinstance(value, str):
            try:
                return DataLayer[value.upper()]
            except KeyError:
                raise ValueError(f"invalid data layer '{value}', must be one of {[dl.name for dl in DataLayer]}") from None
        return value

    @model_validator(mode="after")
    def _default_data_origin(self) -> Self:
        if not self.data_origin:
            self.data_origin = self.data_source
        return self

    def finalize_load_config(
        self,
        io: BaseIO | None,
        data_layer: DataLayer,
    ) -> None:
        """Finalize the io and the data layer the data is stored in.

        because in pipeline mode io is unknown at request construction time
        and only becomes final when .load() is invoked.
        data_layer is the layer to store in; it never changes self.data_layer (the data's own layer).
        """
        if io:
            if data_layer < self.data_layer:
                raise ValueError(f"cannot store {self.data_layer} data in a lower layer {data_layer}")
            if self.data_layer == DataLayer.RAW and data_layer != DataLayer.RAW:
                raise ValueError(f"RAW data is not cleaned, it cannot be stored in the {data_layer} layer")
            if self.is_streaming() and data_layer == DataLayer.RAW:
                raise RuntimeError("Writing raw data in streaming is not supported")
        self.io = io
