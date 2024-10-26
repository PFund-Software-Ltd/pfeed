import os
import yaml


from pfund.datas.resolution import Resolution

from pfeed.const.common import SUPPORTED_DATA_TYPES


class BaseDataSource:
    def __init__(self, name: str):
        self.name = name.upper()
        self.metadata = self._load_metadata()
        self.api_key_required = self.metadata['api_key_required']
        self.has_free_access = self.metadata['has_free_access']
        self.data_origin = self.metadata['data_origin']
        self.rate_limits = self.metadata['rate_limits']
        self.docs_url = self.metadata['docs_url']
        self.data_categories = list(self.metadata['data_categories'].keys())
        self.data_types = self._extract_and_extend_data_types()
        self.product_types = self._extract_product_types()
    
    def _load_metadata(self):
        metadata_file_path = os.path.join(os.path.dirname(__file__), self.name.lower(), 'metadata.yml')
        with open(metadata_file_path, 'r') as file:
            metadata = yaml.safe_load(file)
        return metadata

    def _extract_and_extend_data_types(self):
        '''
        Extract supported data types from metadata and extend them with lower resolutions.
        
        This method iterates through the data categories in the metadata,
        collects all supported data types, converts them to lowercase,
        and automatically extends to include lower time resolutions.
        For example, if 'tick' is supported, then 'second', 'minute', etc. 
        are also added to the supported types.
        '''
        data_types = []
        for data_category in self.metadata['data_categories']:
            for data_type in self.metadata['data_categories'][data_category]:
                data_types.append(data_type.lower().replace('_l', '_L'))
        resolutions = sorted(set([Resolution(data_type) for data_type in data_types]))
        lowest_resolution = resolutions[0]
        for data_type in SUPPORTED_DATA_TYPES:
            resolution = Resolution(data_type)
            if resolution < lowest_resolution:
                data_types.append(data_type)
        return data_types
    
    def _extract_product_types(self):
        return list(set(
            product_type.upper()
            for data_category in self.metadata['data_categories'].values()
            for data_type in data_category.values()
            for product_type in data_type
        ))
