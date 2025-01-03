import os
import shutil
import datetime
from pathlib import Path

from pfeed.storages.local_storage import LocalStorage


class CacheStorage(LocalStorage):
    NUM_RETAINED_DAYS = 7
    
    @staticmethod
    def _get_today() -> datetime.date:
        return datetime.datetime.now(tz=datetime.timezone.utc).date()
    
    def _create_data_path(self) -> Path:
        from pfeed.config import get_config
        config = get_config()
        return Path(config.cache_path)

    def clear_caches(self):
        '''Clear old caches except the current date'''
        from pfund import print_error
        from pfund.utils.utils import get_last_modified_time
        from pfeed.config import get_config
        config = get_config()
        cache_path = config.cache_path
        today = self._get_today()
        
        for file_dir in os.listdir(cache_path):
            full_path = os.path.join(cache_path, file_dir)
            last_modified_date = get_last_modified_time(full_path).date()
            try:
                if os.path.isdir(full_path) and last_modified_date < today - datetime.timedelta(days=self.NUM_RETAINED_DAYS):
                    shutil.rmtree(full_path)
                    print(f"Removed '{file_dir}' directory in caches")
            except Exception as e:
                print_error(f"Error removing '{file_dir}' directory in caches: {str(e)}")
