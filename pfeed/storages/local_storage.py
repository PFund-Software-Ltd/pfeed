from pfeed.types.core import tData, is_dataframe
from pfeed.storages.base_storage import BaseStorage


class LocalStorage(BaseStorage):
    def load(self, data: tData):
        if is_dataframe(data):
            from pfeed.etl import convert_to_pandas_df
            df = convert_to_pandas_df(data)
            df.to_parquet(self.file_path, compression='zstd')
        elif isinstance(data, bytes):
            with open(self.file_path, 'wb') as f:
                f.write(data)
        else:
            raise NotImplementedError(f'{type(data)=}')