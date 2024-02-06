"""Downloads Bybit historical data"""
import os
import logging
import datetime
from collections import defaultdict
from logging.handlers import QueueHandler, QueueListener

from tqdm import tqdm
from rich.console import Console

from pfeed.const.paths import USER_CONFIG_FILE_PATH
from pfeed.cli.commands.config import load_config
from pfeed.config_handler import ConfigHandler
from pfeed.utils.utils import get_dates_in_between
from pfeed.utils.validate import validate_pdts_and_ptypes
from pfeed.const.commons import SUPPORTED_DATA_TYPES
from pfeed.sources.bybit.const import DATA_START_DATE, DATA_SOURCE, SUPPORTED_PRODUCT_TYPES, create_efilename
from pfeed.sources.bybit import api
from pfeed.sources.bybit import etl


logger = logging.getLogger(DATA_SOURCE.lower())
cprint = Console().print


__all__ = ['run']


def create_pdts_using_ptypes(exchange, ptypes) -> list[str]:
    adapter = exchange.adapter
    pdts = []
    for ptype in ptypes:
        assert ptype in SUPPORTED_PRODUCT_TYPES, f'{ptype} is not supported, {SUPPORTED_PRODUCT_TYPES=}'
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


def download_historical_data(
    pdts: list[str] | None=None, 
    dtypes: list[str] | None=None,
    ptypes: list[str] | None=None, 
    start_date: datetime.date | None=None, 
    end_date: datetime.date | None=None,
    batch_size: int=8,
    use_ray: bool=True, 
    use_minio: bool=True,
    config: ConfigHandler | None=None,
):
    from pfund.exchanges.bybit.exchange import Exchange
    from pfund.plogging import set_up_loggers
    
    env = 'LIVE'  # historical data is from LIVE env
    source = DATA_SOURCE
    exchange = Exchange(env)
    adapter = exchange.adapter
    
    cprint(f'PFeed: getting historical data from {source}', style='bold yellow')
    
    if not config:
        config: dict = load_config(USER_CONFIG_FILE_PATH)
        config = ConfigHandler(**config)
    set_up_loggers(f'{config.log_path}/{env}', config.logging_config_file_path, user_logging_config=config.logging_config)
    data_path = config.data_path
    
    dtypes = [dtype.lower() for dtype in dtypes] if dtypes else SUPPORTED_DATA_TYPES[:]
    assert all(dtype in SUPPORTED_DATA_TYPES for dtype in dtypes), f'{dtypes=} but {SUPPORTED_DATA_TYPES=}'
    
    validate_pdts_and_ptypes(source, pdts, ptypes, is_cli=False)
    ptypes = [ptype.upper() for ptype in ptypes] if ptypes else SUPPORTED_PRODUCT_TYPES[:]
    pdts = [pdt.replace('-', '_').upper() for pdt in pdts] if pdts else []
    if not pdts:
        pdts = create_pdts_using_ptypes(exchange, ptypes)
            
    start_date = start_date or datetime.datetime.strptime(DATA_START_DATE, '%Y-%m-%d').date()
    end_date = end_date or datetime.datetime.now(tz=datetime.timezone.utc).date()
    dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
    ray_tasks = defaultdict(list)
    
    cprint(f"Ray is {'enabled' if use_ray else 'disabled'}", style='bold')
    if use_minio and os.getenv('MINIO_ENDPOINT') is None:
        cprint('"MINIO_ENDPOINT" is not found in environment variables, disable MinIO', style='bold')
        use_minio = False
    cprint(f"MinIO is {'enabled' if use_minio else 'disabled'}", style='bold')    

    for pdt in pdts if use_ray else tqdm(pdts, desc=f'Downloading {source} historical data by product', colour='green'):
        ptype = pdt.split('_')[-1]
        is_spot = (ptype.upper() == 'SPOT')
        category = exchange.categorize_product(ptype)
        epdt = adapter(pdt, ref_key=category)
        efilenames = api.get_efilenames(category, epdt)
        for date in dates if use_ray else tqdm(dates, desc=f'Downloading {source} {pdt} historical data by date', colour='yellow'):
            efilename = create_efilename(epdt, date, is_spot=is_spot)
            if efilename not in efilenames:
                # logger.debug(f'{efilename} does not exist in {source}')
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
                    raise Exception(f'failed to download {source} {pdt} {date} historical data')

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
                raise Exception(f'failed to download {source} {pdt} {date} historical data')

        log_queue = Queue()
        QueueListener(log_queue, *logger.handlers, respect_handler_level=True).start()
        for pdt in tqdm(ray_tasks, desc=f'Downloading {source} historical data by product', colour='green'):
            batches = [ray_tasks[pdt][i: i + batch_size] for i in range(0, len(ray_tasks[pdt]), batch_size)]
            for batch in tqdm(batches, desc=f'Downloading {source} {pdt} historical data by batch ({batch_size=})', colour='yellow'):
                futures = [_run_task.remote(log_queue, *task) for task in batch]
                ray.get(futures)
                
    logger.warning(f'finished downloading historical data from {source} to {data_path} or MinIO if enabled')
    
    
run = download_historical_data