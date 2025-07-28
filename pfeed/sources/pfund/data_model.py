import datetime
from pathlib import Path

from pydantic import Field

from pfund.enums import ComponentType
from pfeed.data_models.time_based_data_model import TimeBasedDataModel
from pfeed.sources.pfund.source import PFundSource


class PFundDataModel(TimeBasedDataModel):
    '''
    Data model for pfund to store its components data, e.g. computed features, model predictions, etc.
    '''
    data_source: PFundSource = Field(default_factory=PFundSource)
    component_name: str
    component_type: ComponentType

    def create_filename(self, date: datetime.date | None=None, file_extension='.parquet') -> str:
        return self.component_name + file_extension
    
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