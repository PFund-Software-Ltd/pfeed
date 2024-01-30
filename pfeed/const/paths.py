from pathlib import Path


PROJ_NAME = Path(__file__).resolve().parents[2].name
MAIN_PATH = Path(__file__).resolve().parents[3]
PROJ_PATH = MAIN_PATH / PROJ_NAME / PROJ_NAME
CONFIG_PATH = PROJ_PATH / 'config'
LOG_PATH = MAIN_PATH / PROJ_NAME / 'logs'
DATA_PATH = MAIN_PATH / PROJ_NAME / 'data'