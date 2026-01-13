from __future__ import annotations

from pathlib import Path

from pfund.enums import Environment
from pfund_kit.config import Configuration


__all__ = [
    'get_config',
    'configure',
    'configure_logging',
    'setup_logging',
]


project_name = 'pfeed'
_config: PFeedConfig | None = None
_logging_config: dict | None = None
CONFIGURABLE_KEYS: set[str] = {
    'data_path',
    'log_path',
    'cache_path',
}


def setup_logging(env: Environment | None=None, reset: bool=False):
    from pfund_kit.logging import clear_logging_handlers, setup_exception_logging
    from pfund_kit.logging.configurator import LoggingDictConfigurator
    
    env = Environment[env.upper()] if env else None
    
    if reset:
        clear_logging_handlers()
    
    config: PFeedConfig = get_config()
    logging_config: dict = get_logging_config()

    log_path = config.log_path / env if env else config.log_path
    log_path.mkdir(parents=True, exist_ok=True)

    # ≈ logging.config.dictConfig(logging_config) with a custom configurator
    logging_configurator = LoggingDictConfigurator(log_path=log_path, logging_config=logging_config, lazy=True)
    logging_configurator.configure()
    
    setup_exception_logging(logger_name=project_name)
    return logging_config


def get_config() -> PFeedConfig:
    """Lazy singleton - only creates config when first called.
    Also loads the .env file.
    """
    global _config
    if _config is None:
        _config = PFeedConfig()
    return _config


def get_logging_config() -> dict:
    global _logging_config
    if _logging_config is None:
        _logging_config = configure_logging()
    return _logging_config


def configure(
    data_path: str | None = None,
    log_path: str | None = None,
    cache_path: str | None = None,
    persist: bool = False,
) -> PFeedConfig:
    '''
    Configures the global config object.
    Args:
        data_path: Path to the data directory.
        log_path: Path to the log directory.
        cache_path: Path to the cache directory.
        persist: If True, the config will be saved to the config file.
    '''
    config = get_config()
    config_dict = config.to_dict()
    config_dict.pop('__version__')
    config_dict_keys = set(config_dict.keys())
    assert config_dict_keys == CONFIGURABLE_KEYS, \
        f"Config keys are not the same as CONFIGURABLE_KEYS: {config_dict_keys} != {CONFIGURABLE_KEYS}"
    
    # Apply updates for non-None values
    for k in CONFIGURABLE_KEYS:
        v = locals().get(k)
        if v is not None:
            if '_path' in k:
                v = Path(v)
            setattr(config, k, v)
    
    config.ensure_dirs()
    
    if persist:
        config.save()
        
    return config


def configure_logging(logging_config: dict | None=None, debug: bool=False) -> dict:
    '''
    Loads logging config from YAML file and merges with optional user overrides.

    Args:
        logging_config: Optional dict to override/extend the base YAML config.
        debug: If True, sets all loggers and handlers to DEBUG level.
               This overrides any level settings from the YAML file and logging_config.

    Returns:
        Merged logging config dict.

    Raises:
        FileNotFoundError: If the logging config YAML file is not found.
    '''
    from pfund_kit.utils import deep_merge
    from pfund_kit.logging import enable_debug_logging
    from pfund_kit.utils.yaml import load

    global _logging_config

    config = get_config()

    # load logging.yml file
    logging_config_from_yml: dict | None = load(config.logging_config_file_path)
    if logging_config_from_yml is None:
        raise FileNotFoundError(f"Logging config file {config.logging_config_file_path} not found")
    
    _logging_config = deep_merge(logging_config_from_yml, logging_config or {})
    if debug:
        _logging_config = enable_debug_logging(_logging_config)
    return _logging_config
    

class PFeedConfig(Configuration):
    def __init__(self):
        from pfund_kit.utils import load_env_file
        load_env_file(verbose=False)
        super().__init__(project_name=project_name, source_file=__file__)
    
    # TODO: when compose.yml is in use
    def prepare_docker_context(self):
        pass
        # import os
        # os.environ['MINIO_DATA_PATH'] = str(self.data_path / 'minio')
        # os.environ['TIMESCALEDB_DATA_PATH'] = str(self.data_path / 'timescaledb')