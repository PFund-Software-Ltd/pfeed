from typing import ClassVar

from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from pfeed.enums import DataStorage, DataLayer, IOFormat, Compression


class StorageConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    storage: DataStorage | str=DataStorage.LOCAL
    data_path: Path | str | None=None
    data_layer: DataLayer | str=DataLayer.CLEANED
    data_domain: str=''
    io_format: IOFormat | str=IOFormat.PARQUET
    compression: Compression | str=Compression.SNAPPY
    
    @field_validator('storage', mode='before')
    @classmethod
    def create_storage(cls, v: DataStorage | str) -> DataStorage:
        if not isinstance(v, DataStorage):
            return DataStorage[v.upper()]
        return v
    
    @field_validator('data_path', mode='before')
    @classmethod
    def create_data_path(cls, v: Path | str | None) -> Path:
        from pfeed import get_config
        config = get_config()
        if v is None:
            return config.data_path
        if isinstance(v, str):
            return Path(v)
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
    
    @model_validator(mode='after')
    def resolve_io_format(self):
        '''Handle case where io_format is using a default value e.g. IOFormat.PARQUET, but storage is DuckDBStorage
        need to resolve to the supported io format for the storage if the storage only supports one io format
        raise error if the storage supports multiple io formats and the io format is not supported
        '''
        Storage = DataStorage[self.storage].storage_class
        supported_io_formats = Storage.SUPPORTED_IO_FORMATS
        if self.io_format not in supported_io_formats:
            if len(supported_io_formats) == 1:
                self.io_format = supported_io_formats[0]
            else:
                raise ValueError(f'io_format={self.io_format} is not supported for {Storage.__name__}, supported IO formats: {supported_io_formats}')
        return self
    
    @field_validator('io_format', mode='before')
    @classmethod
    def create_io_format(cls, v: IOFormat | str) -> IOFormat:
        if not isinstance(v, IOFormat):
            return IOFormat[v.upper()]
        return v
    
    @field_validator('compression', mode='before')
    @classmethod
    def create_compression(cls, v: Compression | str) -> Compression:
        if not isinstance(v, Compression):
            return Compression[v.upper()]
        return v
