from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pfeed.enums import DataLayer, DataStorage
from pfeed.utils.file_path import FilePath


class StorageConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra="forbid",
    )

    storage: DataStorage | str = DataStorage.LOCAL
    data_path: FilePath | Path | str | None = Field(default=None, validate_default=True)
    data_layer: DataLayer | str = DataLayer.CLEANED
    data_domain: str = ""
    storage_options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("storage", mode="before")
    @classmethod
    def validate_storage(cls, v: DataStorage | str) -> DataStorage:
        if not isinstance(v, DataStorage):
            return DataStorage[v.upper()]
        return v

    @field_validator("data_path", mode="before")
    @classmethod
    def validate_data_path(cls, v: FilePath | Path | str | None) -> FilePath:
        if v is None:
            from pfeed import get_config

            config = get_config()
            return FilePath(config.data_path)
        elif not isinstance(v, FilePath):
            return FilePath(v)
        else:
            return v

    @field_validator("data_layer", mode="before")
    @classmethod
    def validate_data_layer(cls, v: DataLayer | str) -> DataLayer:
        if isinstance(v, str):
            return DataLayer[v.upper()]
        return v

    @field_validator("data_domain", mode="before")
    @classmethod
    def uppercase_data_domain(cls, v: str) -> str:
        v = v.upper()
        return v
