import pandas as pd

from pfeed.data_handlers.tabular_data_handler import TabularDataHandler


class NewsDataHandler(TabularDataHandler):
    def _validate_schema(self, data: pd.DataFrame) -> pd.DataFrame:
        from pfeed.schemas import NewsDataSchema
        schema = NewsDataSchema
        return schema.validate(data)