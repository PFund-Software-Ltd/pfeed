import os
import sys
from pathlib import Path
import yaml
import multiprocessing
import logging
import shutil
import importlib.resources
from types import TracebackType
from dataclasses import dataclass, asdict

from pfeed.const.paths import (
    PROJ_NAME, 
    LOG_PATH, 
    DATA_PATH,
    CACHE_PATH,
    CONFIG_PATH, 
    CONFIG_FILE_PATH
)


__all__ = [
    'get_config',
    'configure',
]


def _custom_excepthook(exception_class: type[BaseException], exception: BaseException, traceback: TracebackType):
    '''Catches any uncaught exceptions and logs them'''
    # sys.__excepthook__(exception_class, exception, traceback)
    logging.getLogger(PROJ_NAME).exception('Uncaught exception:', exc_info=(exception_class, exception, traceback))


@dataclass
class ConfigHandler:
    data_path: str = str(DATA_PATH)
    log_path: str = str(LOG_PATH)
    cache_path: str = str(CACHE_PATH)
    logging_config_file_path: str = f'{CONFIG_PATH}/logging.yml'
    docker_compose_file_path: str = f'{CONFIG_PATH}/docker-compose.yml'
    fork_process: bool = True
    custom_excepthook: bool = False
    env_file_path: str = f'{CONFIG_PATH}/.env'
    debug: bool = False
    
    _logging_config = {}
    _instance = None
    _verbose = False

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls.load()
        return cls._instance

    @classmethod
    def set_verbose(cls, verbose: bool):
        cls._verbose = verbose
    
    @classmethod
    def load(cls):
        '''Loads user's config file and returns a ConfigHandler object'''
        CONFIG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        if CONFIG_FILE_PATH.is_file():
            with open(CONFIG_FILE_PATH, 'r') as f:
                config = yaml.safe_load(f) or {}
                if cls._verbose:
                    print(f"{PROJ_NAME} config loaded from {CONFIG_FILE_PATH}.")
        else:
            config = {}
        config_handler = cls(**config)
        if not config:
            config_handler.dump()
        return config_handler
    
    @classmethod
    def reset(cls):
        '''Resets the config by deleting the user config directory and reloading the config'''
        shutil.rmtree(CONFIG_PATH)
        if cls._verbose:
            print(f"{PROJ_NAME} config successfully reset.")
        return cls.load()
    
    def dump(self):
        with open(CONFIG_FILE_PATH, 'w') as f:
            yaml.dump(asdict(self), f, default_flow_style=False)
            if self._verbose:
                print(f"{PROJ_NAME} config saved to {CONFIG_FILE_PATH}.")
    
    @property
    def logging_config(self):
        return self._logging_config
    
    @logging_config.setter
    def logging_config(self, value: dict):
        self._logging_config = value
        
    def __post_init__(self):
        self._initialize_files()
        self._initialize_configs()
    
    def _initialize_files(self):
        '''Creates .env and copy logging.yml and docker-compose.yml from package directory to the user config path'''
        package_dir = Path(importlib.resources.files(PROJ_NAME)).resolve().parents[0]
        for path in [self.env_file_path, self.logging_config_file_path, self.docker_compose_file_path]:
            path = Path(path)
            try:
                if not path.exists():
                    if path.name == '.env':
                        path.touch(exist_ok=True)
                    else:
                        shutil.copy(package_dir / path.name, CONFIG_PATH)
            except Exception as e:
                print(f'Error creating or copying {path.name}: {e}')
    
    def _initialize_configs(self):
        for path in [self.data_path, self.cache_path]:
            if not os.path.exists(path):
                os.makedirs(path)
                if self._verbose:
                    print(f'{PROJ_NAME} created {path}')
                
        if self.fork_process and sys.platform != 'win32':
            multiprocessing.set_start_method('fork', force=True)
        
        if self.custom_excepthook and sys.excepthook is sys.__excepthook__:
            sys.excepthook = _custom_excepthook
        
        self.load_env_file(self.env_file_path)
        
        if self.debug:
            self.enable_debug_mode()
    
    def load_env_file(self, env_file_path: str=''):
        from dotenv import find_dotenv, load_dotenv
        if not env_file_path:
            env_file_path = find_dotenv(usecwd=True, raise_error_if_not_found=False)
        
        if env_file_path:
            load_dotenv(env_file_path, override=True)
            if self._verbose:
                print(f'{PROJ_NAME} .env file loaded from {env_file_path}')
        else:
            if self._verbose:
                print(f'{PROJ_NAME} .env file is not found')
            return
    
    def enable_debug_mode(self):
        '''Enables debug mode by setting the log level to DEBUG for all stream handlers'''
        is_loggers_set_up = bool(logging.getLogger(PROJ_NAME).handlers)
        if is_loggers_set_up:
            if self._verbose:
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
    cache_path: str | None = None,
    logging_config_file_path: str | None = None,
    logging_config: dict | None = None,
    docker_compose_file_path: str | None = None,
    env_file_path: str | None = None,
    fork_process: bool | None = None,
    custom_excepthook: bool | None = None,
    debug: bool | None = None,
    verbose: bool = False,
    write: bool = False,
):
    '''Configures the global config object.
    It will override the existing config values from the existing config file or the default values.
    Args:
        write: If True, the config will be saved to the config file.
    '''
    NON_CONFIG_KEYS = ['verbose', 'write']
    config_updates = locals()
    for k in NON_CONFIG_KEYS:
        config_updates.pop(k)
    config_updates.pop('NON_CONFIG_KEYS')

    config = get_config(verbose=verbose)

    # Apply updates for non-None values
    for k, v in config_updates.items():
        if v is not None:
            setattr(config, k, v)
            
    if write:
        config.dump()
        
    config._initialize_configs()
    return config


def get_config(verbose: bool = False) -> ConfigHandler:
    ConfigHandler.set_verbose(verbose)
    return ConfigHandler.get_instance()
