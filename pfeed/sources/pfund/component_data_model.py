from pydantic import field_validator

from pfeed.data_models.market_data_model import MarketDataModel
from pfund.enums import ComponentType


class PFundComponentDataModel(MarketDataModel):
    '''
    Data model for pfund to store its components data, e.g. computed features, model predictions, etc.
    '''
    component_type: ComponentType
    component_name: str
    
    @field_validator('component_name', mode='before')
    @classmethod
    def create_component_name(cls, v):
        from pfund_kit.utils.text import to_snake_case
        return to_snake_case(v)
    