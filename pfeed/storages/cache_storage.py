from typing import Literal

from pfeed.enums import DataLayer
from pfeed.storages.local_storage import LocalStorage
from pfeed.config import get_config


config = get_config()


class CacheStorage(LocalStorage):
    DEFAULT_NUM_RETAINED_DAYS = 7

    def __init__(
        self,
        data_layer: DataLayer=DataLayer.CLEANED,
        data_domain: str | Literal['MARKET_DATA', 'NEWS_DATA'] = 'MARKET_DATA',
        storage_options: dict | None = None,
        num_retained_days: int = DEFAULT_NUM_RETAINED_DAYS,
    ):
        super().__init__(
            data_path=config.cache_path,
            data_layer=data_layer,
            data_domain=data_domain,
            storage_options=storage_options,
        )
        self.num_retained_days = num_retained_days
        self._clear_caches()
    
    def _clear_caches(self):
        '''Clear old caches except the current date'''
        import shutil
        from datetime import timedelta
        from pfund_kit.utils import get_last_modified_time
        from pfund_kit.style import cprint, TextStyle, RichColor
        from pfund_kit.utils.temporal import get_today

        cache_path = config.cache_path
        today = get_today()
        
        if not cache_path.exists():
            return
        for file_dir in cache_path.iterdir():
            if not file_dir.is_dir():
                continue
            last_modified_date = get_last_modified_time(file_dir).date()
            try:
                if last_modified_date < today - timedelta(days=self.num_retained_days):
                    shutil.rmtree(file_dir)
                    cprint(f"Removed '{file_dir}' directory in caches", style=TextStyle.BOLD + RichColor.YELLOW)
            except Exception as e:
                cprint(f"Error removing '{file_dir}' directory in caches: {str(e)}", style=TextStyle.BOLD + RichColor.RED)
