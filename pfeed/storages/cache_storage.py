import shutil
import datetime
from pathlib import Path

from pfeed.typing import tDATA_LAYER
from pfeed.storages import LocalStorage


class CacheStorage(LocalStorage):
    NUM_RETAINED_DAYS = 7

    def __init__(
        self,
        data_layer: tDATA_LAYER='cleaned',
        data_domain: str='general_data',
        use_deltalake: bool=False, 
        **kwargs
    ):
        super().__init__(name='cache', data_layer=data_layer, data_domain=data_domain, use_deltalake=use_deltalake, **kwargs)
        self._clear_caches()
    
    @staticmethod
    def _get_today() -> datetime.date:
        return datetime.datetime.now(tz=datetime.timezone.utc).date()
    
    @property
    def data_path(self) -> Path:
        from pfeed.config import get_config
        config = get_config()
        return (
            Path(config.cache_path)
            / f'data_layer={self.data_layer.name.lower()}'
            / f'data_domain={self.data_domain}'
        )

    def _clear_caches(self):
        '''Clear old caches except the current date'''
        from pfund import print_error
        from pfund.utils.utils import get_last_modified_time
        from pfeed.config import get_config
        config = get_config()
        cache_path = config.cache_path
        today = self._get_today()
        
        for file_dir in Path(cache_path).rglob("*"):
            if not file_dir.is_dir():
                continue
            last_modified_date = get_last_modified_time(file_dir).date()
            try:
                if last_modified_date < today - datetime.timedelta(days=self.NUM_RETAINED_DAYS):
                    shutil.rmtree(file_dir)
                    print(f"Removed '{file_dir}' directory in caches")
            except Exception as e:
                print_error(f"Error removing '{file_dir}' directory in caches: {str(e)}")
