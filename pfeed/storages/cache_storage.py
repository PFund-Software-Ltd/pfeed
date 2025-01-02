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
        from pfeed.config_handler import get_config
        config = get_config()
        today = self._get_today()
        # group the storages by date, easier to be cleaned up
        return Path(config.cache_path) / str(today)

    def clear_caches(self):
        '''Clear old caches except the current date'''
        from pfund import print_error
        from pfeed.config_handler import get_config
        config = get_config()
        cache_path = config.cache_path
        today = self._get_today()
        for date_dir in os.listdir(cache_path):
            full_path = os.path.join(cache_path, date_dir)
            date = datetime.datetime.strptime(date_dir, '%Y-%m-%d').date()
            try:
                if os.path.isdir(full_path) and date < today - datetime.timedelta(days=self.NUM_RETAINED_DAYS):
                    shutil.rmtree(full_path)
                    print(f"Removed '{date_dir}' directory in caches")
            except Exception as e:
                print_error(f"Error removing '{date_dir}' directory in caches: {str(e)}")
