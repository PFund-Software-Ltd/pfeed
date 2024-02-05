import os
import sys
from pathlib import Path
import multiprocessing
import logging
from types import TracebackType
from dataclasses import dataclass

from pfeed.const.paths import PROJ_NAME, CONFIG_PATH, LOG_PATH, DATA_PATH


def _custom_excepthook(exception_class: type[BaseException], exception: BaseException, traceback: TracebackType):
    '''Catches any uncaught exceptions and logs them'''
    # sys.__excepthook__(exception_class, exception, traceback)
    try:
        raise exception
    except:
        logging.getLogger(PROJ_NAME).exception('Uncaught exception:')


@dataclass
class ConfigHandler:
    data_path: Path = DATA_PATH
    log_path: Path = LOG_PATH
    logging_config_file_path: Path = CONFIG_PATH / 'logging.yml'
    logging_config: dict | None = None
    use_fork_process: bool = True
    use_custom_excepthook: bool = True
    
    def __post_init__(self):
        for path in [self.data_path]:
            if not path.exists():
                os.makedirs(path)
                print(f'created {str(path)}')
                
        if self.use_fork_process and sys.platform != 'win32':
            multiprocessing.set_start_method('fork', force=True)
        
        if self.use_custom_excepthook:
            sys.excepthook = _custom_excepthook
            
        
def configure(
    data_path: str | Path=DATA_PATH,
    log_path: str | Path=LOG_PATH,
    logging_config_file_path: str | Path = CONFIG_PATH / 'logging.yml',
    logging_config: dict | None=None,
    use_fork_process: bool=True,
    use_custom_excepthook: bool=True,
):
    logging_config_file_path = Path(logging_config_file_path)
    assert logging_config_file_path.is_file(), f'{logging_config_file_path=} is not a file'
    return ConfigHandler(
        data_path=Path(data_path),
        log_path=Path(log_path),
        logging_config_file_path=logging_config_file_path,
        logging_config=logging_config,
        use_fork_process=use_fork_process,
        use_custom_excepthook=use_custom_excepthook,
    )
