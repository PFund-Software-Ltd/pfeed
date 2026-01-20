import datetime

from pydantic import BaseModel, ConfigDict, field_validator

from pfund.enums import Environment
from pfeed.enums import DataStorage, DataLayer, IOFormat, Compression


class TimeBasedFeedDownloadRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra='forbid', frozen=True)

    env: Environment
    start_date: datetime.date
    end_date: datetime.date
    data_origin: str
    dataflow_per_date: bool
    to_storage: DataStorage
    data_layer: DataLayer
    io_format: IOFormat
    compression: Compression
    
    @field_validator('env', mode='before')
    @classmethod
    def create_env(cls, v):
        if isinstance(v, str):
            return Environment[v.upper()]
        return v
    
    @field_validator('to_storage', mode='before')
    @classmethod
    def create_to_storage(cls, v):
        if isinstance(v, str):
            return DataStorage[v.upper()]
        return v
    
    @field_validator('data_layer', mode='before')
    @classmethod
    def create_data_layer(cls, v):
        if isinstance(v, str):
            return DataLayer[v.upper()]
        return v
