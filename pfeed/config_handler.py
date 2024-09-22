import os
import sys
import multiprocessing
import logging
from types import TracebackType
from dataclasses import dataclass

from pfeed.const.paths import PROJ_NAME, MAIN_PATH, LOG_PATH, DATA_PATH, USER_CONFIG_FILE_PATH


# Global configuration object
_global_config = None
__all__ = [
    'get_config',
    'configure',
]


def _custom_excepthook(exception_class: type[BaseException], exception: BaseException, traceback: TracebackType):
    '''Catches any uncaught exceptions and logs them'''
    # sys.__excepthook__(exception_class, exception, traceback)
    try:
        raise exception
    except:
        logging.getLogger(PROJ_NAME).exception('Uncaught exception:')


@dataclass
class ConfigHandler:
    data_path: str = str(DATA_PATH)
    log_path: str = str(LOG_PATH)
    logging_config_file_path: str = f'{MAIN_PATH}/logging.yml'
    logging_config: dict | None = None
    use_fork_process: bool = True
    use_custom_excepthook: bool = False
    env_file_path: str = ''
    debug: bool = False
    
    @classmethod
    def get_instance(cls):
        global _global_config
        if _global_config is None:
            _global_config = cls.load_config()
        return _global_config
    
    @classmethod
    def load_config(cls):
        import yaml

        '''Loads user's config file and returns a ConfigHandler object'''
        config_file_path = USER_CONFIG_FILE_PATH
        if config_file_path.is_file():
            with open(config_file_path, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
        return cls(**config)
    
    def __post_init__(self):
        self._initialize()
    
    def _initialize(self):
        self.logging_config = self.logging_config or {}
        
        for path in [self.data_path]:
            if not os.path.exists(path):
                os.makedirs(path)
                print(f'created {path}')
                
        if self.use_fork_process and sys.platform != 'win32':
            multiprocessing.set_start_method('fork', force=True)
        
        if self.use_custom_excepthook and sys.excepthook is sys.__excepthook__:
            sys.excepthook = _custom_excepthook
        
        self.load_env_file(self.env_file_path)
        
        if self.debug:
            self.enable_debug_mode()
    
    def load_env_file(self, env_file_path: str=''):
        from dotenv import find_dotenv, load_dotenv
        
        if not env_file_path:
            env_file_path = find_dotenv(usecwd=True, raise_error_if_not_found=False)
            if env_file_path:
                print(f'.env file path is not specified, using env file in "{env_file_path}"')
            else:
                # print('.env file is not found')
                return
        load_dotenv(env_file_path, override=True)
    
    def enable_debug_mode(self):
        '''Enables debug mode by setting the log level to DEBUG for all stream handlers'''
        is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
        if is_loggers_set_up:
            print('loggers are already set up, ignoring debug mode')
            return
        if 'handlers' not in self.logging_config:
            self.logging_config['handlers'] = {}
        for handler in ['stream_handler', 'stream_path_handler']:
            if handler not in self.logging_config['handlers']:
                self.logging_config['handlers'][handler] = {}
            self.logging_config['handlers'][handler]['level'] = 'DEBUG'


def configure(
    data_path: str | None = None,
    log_path: str | None = None,
    logging_config_file_path: str | None = None,
    logging_config: dict | None = None,
    use_fork_process: bool | None = None,
    use_custom_excepthook: bool | None = None,
    env_file_path: str | None = None,
    debug: bool | None = None,
    **kwargs,
):
    '''Configures the global config object.
    It will override the existing config values from the existing config file or the default values.
    '''
    global _global_config
    _global_config = get_config()
    
    # override config values, if provided
    if data_path is not None:
        _global_config.data_path = data_path
    if log_path is not None:
        _global_config.log_path = log_path
    if logging_config_file_path is not None:
        _global_config.logging_config_file_path = logging_config_file_path
    if logging_config is not None:
        _global_config.logging_config = logging_config
    if use_fork_process is not None:
        _global_config.use_fork_process = use_fork_process
    if use_custom_excepthook is not None:
        _global_config.use_custom_excepthook = use_custom_excepthook
    if env_file_path is not None:
        _global_config.env_file_path = env_file_path
    if debug is not None:
        _global_config.debug = debug
            
    for k, v in kwargs.items():
        if hasattr(_global_config, k):
            setattr(_global_config, k, v)
        else:
            raise AttributeError(f'{k} is not an attribute of ConfigHandler')
    
    _global_config._initialize()
    return _global_config


def get_config() -> ConfigHandler:
    return ConfigHandler.get_instance()
