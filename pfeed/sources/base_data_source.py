from pfeed.types.literals import tDATA_SOURCE
from pfeed.const.enums import DataSource


class BaseDataSource:
    def __init__(self, name: tDATA_SOURCE):
        self.name = DataSource[name.upper()]
        self.generic_metadata, self.specific_metadata = self._load_metadata()
        self.api_key_required = self.generic_metadata['api_key_required']
        self.has_free_access = self.generic_metadata['has_free_access']
        self.data_origin = self.generic_metadata['data_origin']
        self.rate_limits = self.generic_metadata['rate_limits']
        self.docs_url = self.generic_metadata['docs_url']
        self.data_categories = list(self.generic_metadata['data_categories'].keys())
        self.dtypes = self.data_types = self._extract_data_types()
        self.highest_resolution, self.lowest_resolution = self._get_highest_and_lowest_resolutions(self.data_types)
        self.ptypes = self.product_types = self._extract_product_types()
    
    @staticmethod
    def _get_highest_and_lowest_resolutions(data_types: list[str]):
        from pfund.datas.resolution import Resolution
        from pfeed.const.enums import MarketDataType
        resolutions = sorted([Resolution(data_type) for data_type in data_types if data_type.upper() in MarketDataType.__members__], reverse=True)
        return resolutions[0], resolutions[-1]
    
    def _load_metadata(self):
        import os
        import yaml
        metadata_file_path = os.path.join(os.path.dirname(__file__), self.name.lower(), 'metadata.yml')
        with open(metadata_file_path, 'r') as file:
            metadata_docs = yaml.safe_load_all(file)
            generic_metadata = next(metadata_docs)
            specific_metadata = next(metadata_docs, {})
        return generic_metadata, specific_metadata

    def _extract_data_types(self):
        data_types = []
        for data_category in self.generic_metadata['data_categories']:
            for data_type in self.generic_metadata['data_categories'][data_category]:
                if data_category == 'market_data':
                    # capitalize orderbook_level from 'l' to 'L'
                    data_type = data_type.lower().replace('_l', '_L')
                data_types.append(data_type)
        return data_types
        
    def _extract_product_types(self):
        return list(set(
            product_type.upper()
            for data_category in self.generic_metadata['data_categories'].values()
            for data_type in data_category.values()
            for product_type in data_type
        ))
