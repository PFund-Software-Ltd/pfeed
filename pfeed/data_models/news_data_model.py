from pydantic import model_validator

from pfund.products.product_base import BaseProduct

from pfeed.data_models.time_based_data_model import TimeBasedDataModel


class NewsDataModel(TimeBasedDataModel):
    product: BaseProduct

    @model_validator(mode='before')
    @classmethod
    def check_and_convert(cls, data: dict) -> dict:
        if 'product' in data:
            product = data['product']
            assert isinstance(product, BaseProduct), f'product must be a Product object, got {type(product)}'
        return data
    
    def __hash__(self):
        return hash((
            self.source.name, 
            self.data_origin, 
            self.start_date, 
            self.end_date, 
            self.product
        ))