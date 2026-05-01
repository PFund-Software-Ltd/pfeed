from typing import ClassVar, Any

from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator, Field

from pfeed.utils.file_path import FilePath
from pfeed.enums.data_storage import FileBasedDataStorage
from pfeed.enums import DataStorage, DataLayer


class StorageConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    storage: DataStorage | str = DataStorage.LOCAL
    data_path: FilePath | Path | str | None = Field(default=None, validate_default=True)
    data_layer: DataLayer | str = DataLayer.CLEANED
    data_domain: str=''
    file_backend: FileBasedDataStorage | None = Field(
        default=None,
        description='''
        File-based storage backend, only applicable for file-based storages, 
        e.g. storage=duckdb, file_backend=local or huggingface etc.
        ''',
    )
    storage_options: dict[str, Any] = Field(default_factory=dict)
    
    @field_validator('storage', mode='before')
    @classmethod
    def create_storage(cls, v: DataStorage | str) -> DataStorage:
        if not isinstance(v, DataStorage):
            return DataStorage[v.upper()]
        return v
    
    @field_validator('data_path', mode='before')
    @classmethod
    def create_data_path(cls, v: FilePath | Path | str | None) -> FilePath:
        if v is None:
            from pfeed import get_config
            config = get_config()
            return FilePath(config.data_path)
        elif not isinstance(v, FilePath):
            return FilePath(v)
        else:
            return v
    
    @field_validator('data_layer', mode='before')
    @classmethod
    def create_data_layer(cls, v: DataLayer | str) -> DataLayer:
        if isinstance(v, str):
            return DataLayer[v.upper()]
        return v
    
    @field_validator('data_domain', mode='before')
    @classmethod
    def uppercase_data_domain(cls, v: str) -> str:
        v = v.upper()
        return v
