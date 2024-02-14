import os
import sys
import multiprocessing
import logging
from types import TracebackType
from dataclasses import dataclass

import yaml

from pfeed.const.paths import PROJ_NAME, PROJ_CONFIG_PATH, LOG_PATH, DATA_PATH, USER_CONFIG_FILE_PATH


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
    logging_config_file_path: str = f'{PROJ_CONFIG_PATH}/logging.yml'
    logging_config: dict | None = None
    use_fork_process: bool = True
    use_custom_excepthook: bool = True
    
    @classmethod
    def load_config(cls):
        config_file_path = USER_CONFIG_FILE_PATH
        if config_file_path.is_file():
            with open(config_file_path, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
        return cls(**config)
    
    def __post_init__(self):
        self.logging_config = self.logging_config or {}
        
        for path in [self.data_path]:
            if not os.path.exists(path):
                os.makedirs(path)
                print(f'created {path}')
                
        if self.use_fork_process and sys.platform != 'win32':
            multiprocessing.set_start_method('fork', force=True)
        
        if self.use_custom_excepthook:
            sys.excepthook = _custom_excepthook
            
        
def configure(
    data_path: str = str(DATA_PATH),
    log_path: str = str(LOG_PATH),
    logging_config_file_path: str = f'{PROJ_CONFIG_PATH}/logging.yml',
    logging_config: dict | None=None,
    use_fork_process: bool=True,
    use_custom_excepthook: bool=True,
):
    return ConfigHandler(
        data_path=data_path,
        log_path=log_path,
        logging_config_file_path=logging_config_file_path,
        logging_config=logging_config,
        use_fork_process=use_fork_process,
        use_custom_excepthook=use_custom_excepthook,
    )
