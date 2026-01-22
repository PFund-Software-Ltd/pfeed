import shutil
import datetime
from pathlib import Path

from pfeed.enums import DataLayer
from pfeed.storages.local_storage import LocalStorage


class CacheStorage(LocalStorage):
    # FIXME: move to init?
    NUM_RETAINED_DAYS = 7

    def __init__(
        self,
        data_layer: DataLayer,
        data_path: Path | None = None,
        storage_options: dict | None=None,
    ):
        super().__init__(
            data_layer=data_layer,
            data_path=data_path,
            storage_options=storage_options,
        )
        self._clear_caches()
    
    def _clear_caches(self):
        '''Clear old caches except the current date'''
        from pfund import print_error
        from pfund_kit.utils import get_last_modified_time
        from pfund_kit.utils.temporal import get_today
        from pfeed.config import get_config
        config = get_config()
        cache_path = config.cache_path
        today = get_today()
        
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
