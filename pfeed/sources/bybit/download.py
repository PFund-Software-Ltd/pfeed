"""Downloads Bybit historical data"""
import os
import logging
import datetime
from collections import defaultdict
from logging.handlers import QueueHandler, QueueListener

from tqdm import tqdm
from rich.console import Console

from pfund.products.product_base import BaseProduct
from pfeed.config_handler import ConfigHandler
from pfeed.utils.utils import get_dates_in_between
from pfeed.utils.validate import validate_pdts_and_ptypes
from pfeed.const.commons import SUPPORTED_DATA_TYPES
from pfeed.sources.bybit.const import DATA_START_DATE, DATA_SOURCE, SUPPORTED_PRODUCT_TYPES, create_efilename
from pfeed.sources.bybit import api
from pfeed.sources.bybit import etl
from pfeed.datastore import check_if_minio_running


logger = logging.getLogger(DATA_SOURCE.lower())
cprint = Console().print


__all__ = ['download_historical_data']


def create_pdts_using_ptypes(exchange, ptypes) -> list[str]:
    adapter = exchange.adapter
    pdts = []
    for ptype in ptypes:
        category = exchange.categorize_product_type(ptype)
        epdts = api.get_epdts(category, ptype)
        # NOTE: if adapter(epdt, ref_key=category) == epdt, i.e. key is not found in pdt matching, meaning the product has been delisted
        pdts.extend([adapter(epdt, ref_key=category) for epdt in epdts if adapter(epdt, ref_key=category) != epdt])
    return pdts


def run_etl(product: BaseProduct, date, dtypes, data_path, use_minio):
    category, pdt, epdt = product.category, product.pdt, product.epdt
    if raw_data := api.get_data(category, epdt, date):
        # EXTEND: currently the transformations are fixed and not independent, maybe allow user to pass in custom transformation functions?
        tick_data: bytes = etl.clean_data(category, raw_data)
        second_data: bytes = etl.resample_data(tick_data, resolution='1s', is_tick=True, category=category)
        minute_data: bytes = etl.resample_data(second_data, resolution='1m')
        hour_data: bytes = etl.resample_data(minute_data, resolution='1h')
        daily_data: bytes = etl.resample_data(hour_data, resolution='1d')
        logger.debug(f'resampled {DATA_SOURCE} {pdt} {date} data')
        resampled_datas = {
            'raw': raw_data,
            'tick': tick_data,
            'second': second_data,
            'minute': minute_data,
            'hour': hour_data,
            'daily': daily_data,
        }
        for dtype in dtypes:
            data = resampled_datas[dtype]
            etl.load_data('historical', dtype, pdt, date, data, data_path=data_path, use_minio=use_minio)
    else:
        raise Exception(f'failed to download {DATA_SOURCE} {pdt} {date} historical data')


def download_historical_data(
    pdts: list[str] | None=None, 
    dtypes: list[str] | None=None,
    ptypes: list[str] | None=None, 
    start_date: datetime.date | None=None, 
    end_date: datetime.date | None=None,
    batch_size: int=8,
    use_ray: bool=True,
    use_minio: bool=True,
    debug: bool=False,
    config: ConfigHandler | None=None,
):
    from pfund.exchanges.bybit.exchange import Exchange
    from pfund.plogging import set_up_loggers
    
    # setup
    source = DATA_SOURCE
    exchange = Exchange(env='LIVE')
    adapter = exchange.adapter
    
    # configure
    if not config:
        config = ConfigHandler.load_config()
        
    if debug:
        if 'handlers' not in config.logging_config:
            config.logging_config['handlers'] = {}
        config.logging_config['handlers']['stream_handler'] = {'level': 'DEBUG'}
    set_up_loggers(f'{config.log_path}/{os.getenv("PFEED_ENV", "DEV")}', config.logging_config_file_path, user_logging_config=config.logging_config)
    data_path = config.data_path
    
    # prepare dtypes
    dtypes = [dtype.lower() for dtype in dtypes] if dtypes else SUPPORTED_DATA_TYPES[:]
    assert all(dtype in SUPPORTED_DATA_TYPES for dtype in dtypes), f'{dtypes=} but {SUPPORTED_DATA_TYPES=}'
    
    # prepare pdts
    validate_pdts_and_ptypes(source, pdts, ptypes, is_cli=False)
    ptypes = [ptype.upper() for ptype in ptypes] if ptypes else SUPPORTED_PRODUCT_TYPES[:]
    pdts = [pdt.replace('-', '_').upper() for pdt in pdts] if pdts else []
    if not pdts:
        pdts = create_pdts_using_ptypes(exchange, ptypes)
    
    # prepare dates
    start_date = start_date or datetime.datetime.strptime(DATA_START_DATE, '%Y-%m-%d').date()
    end_date = end_date or datetime.datetime.now(tz=datetime.timezone.utc).date()
    dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
    
    # check if use Ray
    ray_tasks = defaultdict(list)
    cprint(f"Ray is {'enabled' if use_ray else 'disabled'}", style='bold')
    
    # check if use MinIO
    if use_minio:
        assert check_if_minio_running(), "MinIO is not running or not detected"
    cprint(f"MinIO is {'enabled' if use_minio else 'disabled'}", style='bold')

    cprint(f'PFeed: downloading historical data from {source}, {start_date=} {end_date=}', style='bold yellow')
    for pdt in pdts if use_ray else tqdm(pdts, desc=f'Downloading {source} historical data by product', colour='green'):
        product = exchange.create_product(*pdt.split('_'))
        category = product.category
        epdt = adapter(pdt, ref_key=category)
        product.epdt = epdt  # HACK: add epdt to product for convenience
        efilenames = api.get_efilenames(category, epdt)
        # check if the efilename created by the date exists in the efilenames (files on the data server)
        dates = [date for date in dates if create_efilename(epdt, date, is_spot=product.is_spot()) in efilenames]
        for date in dates if use_ray else tqdm(dates, desc=f'Downloading {source} {pdt} historical data by date', colour='yellow'):
            if use_ray:
                ray_tasks[pdt].append((product, date))
            else:
                run_etl(product, date, dtypes, data_path, use_minio)

    if use_ray:
        import ray
        from ray.util.queue import Queue

        @ray.remote
        def _run_task(log_queue: Queue, product: BaseProduct, date: str):
            if not logger.handlers:
                logger.addHandler(QueueHandler(log_queue))
                logger.setLevel(logging.DEBUG)
            run_etl(product, date, dtypes, data_path, use_minio)

        log_queue = Queue()
        QueueListener(log_queue, *logger.handlers, respect_handler_level=True).start()
        for pdt in tqdm(ray_tasks, desc=f'Downloading {source} historical data by product', colour='green'):
            batches = [ray_tasks[pdt][i: i + batch_size] for i in range(0, len(ray_tasks[pdt]), batch_size)]
            for batch in tqdm(batches, desc=f'Downloading {source} {pdt} historical data by batch ({batch_size=})', colour='yellow'):
                futures = [_run_task.remote(log_queue, *task) for task in batch]
                ray.get(futures)
                
    logger.warning(f'finished downloading {source} historical data to {data_path}')
    
    
run = download_historical_data