from datetime import datetime
from pathlib import Path

from pydantic import Field

from pfund.enums import ComponentType
from pfeed.sources._pfund_source import PFundSource
from pfeed.data_models.time_based_data_model import TimeBasedDataModel


class PFundDataModel(TimeBasedDataModel):
    '''
    Data model for pfund to store its components data, e.g. computed features, model predictions, etc.
    '''
    data_source: PFundSource = Field(default_factory=PFundSource)
    component_name: str
    component_type: ComponentType
    file_extension: str = '.parquet'
    compression: str = 'snappy'

    def create_filename(self, date: datetime.date | None=None) -> str:
        return self.component_name + self.file_extension
    
    def create_storage_path(self, date: datetime.date | None=None) -> Path:
        return (
            Path(f'env={self.env.value}')
            / f'data_source={self.data_source.name}'
            / f'data_origin={self.data_origin}'
            / f'component_type={self.component_type.value}'
        )
    
    # TODO:
    def to_metadata(self) -> dict:
        pass