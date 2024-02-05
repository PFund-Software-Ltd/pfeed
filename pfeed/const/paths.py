from pathlib import Path
from platformdirs import user_log_dir, user_data_dir


PROJ_NAME = Path(__file__).resolve().parents[2].name
MAIN_PATH = Path(__file__).resolve().parents[3]
PROJ_PATH = MAIN_PATH / PROJ_NAME / PROJ_NAME
CONFIG_PATH = PROJ_PATH / 'config'
LOG_PATH = Path(user_log_dir()) / PROJ_NAME
DATA_PATH = Path(user_data_dir()) / PROJ_NAME