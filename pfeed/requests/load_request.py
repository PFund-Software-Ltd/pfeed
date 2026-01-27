from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from pfeed.enums import DataStorage, DataLayer, IOFormat, Compression


class LoadRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid')

    storage: DataStorage
    data_path: Path | str | None
    data_layer: DataLayer
    data_domain: str
    io_format: IOFormat
    compression: Compression
    is_validate_data_domain: bool = True
    
    @field_validator('storage', mode='before')
    @classmethod
    def create_storage(cls, v):
        if isinstance(v, str):
            return DataStorage[v.upper()]
        return v
    
    @field_validator('data_layer', mode='before')
    @classmethod
    def create_data_layer(cls, v):
        if isinstance(v, str):
            return DataLayer[v.upper()]
        return v
    
    @field_validator('data_domain', mode='before')
    @classmethod
    def uppercase_data_domain(cls, v):
        v = v.upper()
        return v
    
    @model_validator(mode='after')
    def validate_data_domain(self):
        if self.is_validate_data_domain and self.data_domain and self.data_layer != DataLayer.CURATED:
            raise ValueError(f'custom data_domain={self.data_domain} is only allowed when data layer is CURATED, but got data_layer={self.data_layer}')
        return self
    
    @model_validator(mode='after')
    def resolve_io_format(self):
        '''Handle case where io_format is using a default value e.g. IOFormat.PARQUET, but storage is DuckDBStorage
        need to resolve to the supported io format for the storage if the storage only supports one io format
        raise error if the storage supports multiple io formats and the io format is not supported
        '''
        if self.storage is None:
            return self
        Storage = self.storage.storage_class
        supported_io_formats = Storage.SUPPORTED_IO_FORMATS
        if self.io_format not in supported_io_formats:
            if len(supported_io_formats) == 1:
                self.io_format = supported_io_formats[0]
            else:
                raise ValueError(f'io_format={self.io_format} is not supported for {Storage.__name__}, supported IO formats: {supported_io_formats}')
        return self
    
    @field_validator('io_format', mode='before')
    @classmethod
    def create_io_format(cls, v):
        if isinstance(v, str):
            return IOFormat[v.upper()]
        return v
    
    @field_validator('compression', mode='before')
    @classmethod
    def create_compression(cls, v):
        if isinstance(v, str):
            return Compression[v.upper()]
        return v
