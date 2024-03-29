import os
import logging
import importlib

from pfeed.config_handler import ConfigHandler


class BaseFeed:
    def __init__(self, name: str, config: ConfigHandler | None=None):
        from pfund.plogging import set_up_loggers

        self.name = name.upper()

        # configure
        if not config:
            config = ConfigHandler.load_config()
        set_up_loggers(config.log_path, config.logging_config_file_path, user_logging_config=config.logging_config)
        self.logger = logging.getLogger(self.name.lower())
        self._config = config
        self.data_path = config.data_path
        
    def _derive_dtype_from_resolution(self, resolution: str):
        from pfund.datas.resolution import Resolution
        
        # HACK: mixing resolution with dtype for convenience
        if resolution.startswith('raw'):
            SUPPORTED_RAW_DATA_TYPES = getattr(importlib.import_module(f'pfeed.sources.{self.name.lower()}.const'), 'SUPPORTED_RAW_DATA_TYPES')
            dtype = resolution
            # e.g. convert 'raw' to 'raw_tick'
            if dtype == 'raw':
                dtype = SUPPORTED_RAW_DATA_TYPES[0]
            assert dtype in SUPPORTED_RAW_DATA_TYPES, f'{dtype=} is not supported for {self.name} data'
            return dtype
        else:
            resolution = Resolution(resolution)
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