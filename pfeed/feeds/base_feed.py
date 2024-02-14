import os

from pfeed.config_handler import ConfigHandler


class BaseFeed:
    def __init__(self, name: str, config: ConfigHandler | None=None):
        from pfund.plogging import set_up_loggers

        self.name = name.upper()

        # configure
        if not config:
            config = ConfigHandler.load_config()
        set_up_loggers(f'{config.log_path}/{os.getenv("PFEED_ENV", "DEV")}', config.logging_config_file_path, user_logging_config=config.logging_config)
        self._config = config
        self.data_path = config.data_path