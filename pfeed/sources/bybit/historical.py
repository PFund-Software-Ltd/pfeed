import os
import logging
import datetime
from collections import defaultdict
from logging.handlers import QueueHandler, QueueListener

import yaml
from tqdm import tqdm

from pfeed.utils.utils import get_dates_in_between
from pfeed.const.commons import SUPPORTED_DATA_TYPES
from pfeed.const.paths import PROJ_PATH, DATA_PATH, LOG_PATH, CONFIG_PATH
from pfeed.sources.bybit.const import DATA_START_DATE, DATA_SOURCE, SUPPORTED_CRYPTO_PRODUCT_TYPES, create_efilename
from pfeed.sources.bybit import api
from pfeed.sources.bybit import etl


logger = logging.getLogger(DATA_SOURCE.lower())
__all__ = ['run']


def _load_config():
    file_path = f'{PROJ_PATH}/sources/{DATA_SOURCE.lower()}/config.yml'
    short_file_path = '/'.join(file_path.split("/")[-4:])
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            print(f'loaded config {short_file_path}')
            return yaml.safe_load(f.read())


def get_pdts(exchange, pdts, ptypes) -> list[str]:
    if not pdts:
        adapter = exchange.adapter
        pdts = []
        if not ptypes:
            ptypes = SUPPORTED_CRYPTO_PRODUCT_TYPES
        for ptype in ptypes:
            assert ptype in SUPPORTED_CRYPTO_PRODUCT_TYPES, f'{ptype} is not supported, {SUPPORTED_CRYPTO_PRODUCT_TYPES=}'
            category = exchange.categorize_product(ptype)
            epdts = api.get_epdts(category, ptype)
            # NOTE: if adapter(epdt, ref_key=category) == epdt, i.e. key is not found in pdt matching, meaning the product has been delisted
            pdts.extend([adapter(epdt, ref_key=category) for epdt in epdts if adapter(epdt, ref_key=category) != epdt])
    return pdts


def resample_raw_data(category: str, pdt: str, date: str, raw_data: bytes) -> dict[str, bytes]:
    tick_data = etl.clean_data(category, raw_data)
    second_data = etl.resample_data(tick_data, resolution='1s', is_tick=True, category=category)
    minute_data = etl.resample_data(second_data, resolution='1m')
    hour_data = etl.resample_data(minute_data, resolution='1h')
    daily_data = etl.resample_data(hour_data, resolution='1d')
    logger.debug(f'resampled {DATA_SOURCE} {pdt} {date} data')
    return {
        'raw': raw_data,
        'tick': tick_data,
        'second': second_data,
        'minute': minute_data,
        'hour': hour_data,
        'daily': daily_data,
    }


def run(
    dtypes: list[str] | None=None,
    ptypes: list[str] | None=None, 
    pdts: list[str] | None=None, 
    start_date: str | None=None, 
    end_date: str | None=None, 
    log_path: str=str(LOG_PATH),
    data_path: str=str(DATA_PATH),
    batch_size: int=8,
    use_ray: bool=True, 
    use_minio: bool=True
):
    from pfund.exchanges.bybit.exchange import Exchange
    from pfund.logging import set_up_loggers
    from pfeed import cprint
    
    env = 'LIVE'  # historical data is from LIVE env
    mode = 'historical'
    dtypes = [dtype.lower() for dtype in dtypes] if dtypes else SUPPORTED_DATA_TYPES[:]
    assert all(dtype in SUPPORTED_DATA_TYPES for dtype in dtypes), f'{dtypes=} but {SUPPORTED_DATA_TYPES=}'
    ptypes = [ptype.upper() for ptype in ptypes] if ptypes else []
    pdts = [pdt.upper() for pdt in pdts] if pdts else []
    set_up_loggers(log_path=f'{log_path}/{env}', config_path=CONFIG_PATH)
        
    cprint(f'PFeed ({DATA_SOURCE} {env} server): getting {mode.upper()} data', style='bold yellow')
    
    # load config if any
    if config := _load_config():
        ptypes = ptypes or config.get('ptypes', None)
        pdts = pdts or config.get('pdts', None)
        start_date = start_date or config.get('start_date', None)
        end_date = end_date or config.get('end_date', None)
        
    exchange = Exchange(env)
    adapter = exchange.adapter
    pdts = get_pdts(exchange, pdts, ptypes)
    start_date: str = start_date or DATA_START_DATE
    end_date: str = end_date or datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d')
    dates: list[str] = get_dates_in_between(start_date, end_date)
    ray_tasks = defaultdict(list)

    for pdt in pdts if use_ray else tqdm(pdts, desc=f'Downloading {DATA_SOURCE} historical data by product', colour='green'):
        ptype = pdt.split('_')[-1]
        is_spot = (ptype.upper() == 'SPOT')
        category = exchange.categorize_product(ptype)
        epdt = adapter(pdt, ref_key=category)
        efilenames = api.get_efilenames(category, epdt)
        for date in dates if use_ray else tqdm(dates, desc=f'Downloading {DATA_SOURCE} {pdt} historical data by date', colour='yellow'):
            efilename = create_efilename(epdt, date, is_spot=is_spot)
            if efilename not in efilenames:
                # logger.debug(f'{efilename} does not exist in {DATA_SOURCE}')
                continue
            if use_ray:
                ray_tasks[pdt].append((category, pdt, date))
            else:
                # run ETL = Extract, Transform, Load data
                if raw_data := api.get_data(category, epdt, date):
                    resampled_datas = resample_raw_data(category, pdt, date, raw_data)
                    for dtype in dtypes:
                        etl.load_data(pdt, date, dtype, resampled_datas[dtype], data_path=data_path, use_minio=use_minio)
                else:
                    raise Exception(f'failed to download {DATA_SOURCE} {pdt} {date} historical data')

    if use_ray:
        import ray
        from ray.util.queue import Queue

        @ray.remote
        def _run_task(log_queue: Queue, category: str, pdt: str, date: str):
            if not logger.handlers:
                logger.addHandler(QueueHandler(log_queue))
                logger.setLevel(logging.DEBUG)
            epdt = adapter(pdt, ref_key=category)
            if raw_data := api.get_data(category, epdt, date):
                resampled_datas = resample_raw_data(category, pdt, date, raw_data)
                for dtype in dtypes:
                    etl.load_data(pdt, date, dtype, resampled_datas[dtype], data_path=data_path, use_minio=use_minio)
            else:
                raise Exception(f'failed to download {DATA_SOURCE} {pdt} {date} historical data')

        log_queue = Queue()
        QueueListener(log_queue, *logger.handlers, respect_handler_level=True).start()
        for pdt in tqdm(ray_tasks, desc=f'Downloading {DATA_SOURCE} historical data by product', colour='green'):
            batches = [ray_tasks[pdt][i: i + batch_size] for i in range(0, len(ray_tasks[pdt]), batch_size)]
            for batch in tqdm(batches, desc=f'Downloading {DATA_SOURCE} {pdt} historical data by batch ({batch_size=})', colour='yellow'):
                futures = [_run_task.remote(log_queue, *task) for task in batch]
                ray.get(futures)
                
    logger.warning(f'finished downloading historical data from {DATA_SOURCE} to {data_path} or MinIO if enabled')