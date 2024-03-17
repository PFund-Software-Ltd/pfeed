import os
import logging

from pfeed.config_handler import ConfigHandler
from pfund.datas.resolution import Resolution


class BaseFeed:
    def __init__(self, name: str, config: ConfigHandler | None=None):
        from pfund.plogging import set_up_loggers

        self.name = name.upper()

        # configure
        if not config:
            config = ConfigHandler.load_config()
        set_up_loggers(f'{config.log_path}/{os.getenv("PFEED_ENV", "DEV")}', config.logging_config_file_path, user_logging_config=config.logging_config)
        self.logger = logging.getLogger(self.name.lower())
        self._config = config
        self.data_path = config.data_path
        
    @staticmethod
    def _derive_dtype_from_resolution(resolution: Resolution):
        if resolution.is_tick():
            assert resolution.period == 1, f'{resolution=} is not supported'
            return 'tick'
        elif resolution.is_second():
            return 'second'
        elif resolution.is_minute():
            return 'minute'
        elif resolution.is_hour():
            return 'hour'
        elif resolution.is_day():
            return 'daily'
        else:
            raise Exception(f'{resolution=} is not supported')