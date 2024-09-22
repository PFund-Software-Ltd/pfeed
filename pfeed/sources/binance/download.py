"""Downloads Binance historical data"""
import os
import logging
import datetime
from collections import defaultdict
from logging.handlers import QueueHandler, QueueListener

from tqdm import tqdm
from rich.console import Console

from pfeed import etl
from pfeed.config_handler import ConfigHandler
from pfeed.utils.utils import get_dates_in_between
from pfeed.utils.validate import validate_pdts_and_ptypes
from pfeed.const.common import SUPPORTED_DATA_TYPES
from pfeed.sources.binance.const import DATA_START_DATE, DATA_SOURCE, SUPPORTED_RAW_DATA_TYPES, SUPPORTED_PRODUCT_TYPES, PTYPE_TO_CATEGORY, create_efilename
from pfeed.sources.binance import api
from pfund.products.product_base import BaseProduct
from pfund.exchanges.binance.exchange import Exchange
from pfund.plogging import set_up_loggers


logger = logging.getLogger(DATA_SOURCE.lower() + '_data')
cprint = Console().print


__all__ = ['download_historical_data']


def create_pdts_using_ptypes(ptypes) -> list[str]:
    pdts = []
    for ptype in ptypes:
        exchange = Exchange(env='LIVE', ptype=ptype)
        adapter = exchange.adapter
        category = PTYPE_TO_CATEGORY(ptype)
        epdts = api.get_epdts(ptype)
        # NOTE: if adapter(epdt, ref_key=category) == epdt, i.e. key is not found in pdt matching, meaning the product has been delisted
        pdts.extend([adapter(epdt, ref_key=category) for epdt in epdts if adapter(epdt, ref_key=category) != epdt])
    return pdts


def run_etl(product: BaseProduct, date, dtypes, use_minio):
    pdt = product.pdt
    if raw_data := api.get_data(pdt, date):
        raw_tick: bytes = etl.clean_raw_data(DATA_SOURCE, raw_data)
        tick_data: bytes = etl.clean_raw_tick_data(raw_tick)
        second_data: bytes = etl.resample_data(tick_data, resolution='1s')
        minute_data: bytes = etl.resample_data(second_data, resolution='1m')
        hour_data: bytes = etl.resample_data(minute_data, resolution='1h')
        daily_data: bytes = etl.resample_data(hour_data, resolution='1d')
        logger.debug(f'resampled {DATA_SOURCE} {pdt} {date} data')
        resampled_datas = {
            'raw_tick': raw_tick,
            'tick': tick_data,
            'second': second_data,
            'minute': minute_data,
            'hour': hour_data,
            'daily': daily_data,
        }
        for dtype in dtypes:
            data: bytes = resampled_datas[dtype]
            storage = 'minio' if use_minio else 'local'
            etl.load_data(storage, DATA_SOURCE, data, dtype, pdt, date, mode='historical')
    else:
        raise Exception(f'failed to download {DATA_SOURCE} {pdt} {date} historical data')


def download_historical_data(
    pdts: str | list[str] | None=None, 
    dtypes: str | list[str] | None=None,
    ptypes: str | list[str] | None=None, 
    start_date: str | None=None,
    end_date: str | None=None,
    num_cpus: int=8,
    use_ray: bool=True,
    use_minio: bool=False,
    debug: bool=False,
    config: ConfigHandler | None=None,
) -> None:
    # setup
    source = DATA_SOURCE
    
    # configure
    if not config:
        config = ConfigHandler.load_config()
        
    print(f'''Hint: 
        You can use the command "pfeed config --data-path ..." to set your data path that stores downloaded data.
        The current data path is: {config.data_path}.
    ''')
    
    # make stream_handler level INFO if not debug, default level is DEBUG in logging.yml
    if not debug:
        if 'handlers' not in config.logging_config:
            config.logging_config['handlers'] = {}
        config.logging_config['handlers']['stream_handler'] = {'level': 'INFO'}
    set_up_loggers(config.log_path, config.logging_config_file_path, user_logging_config=config.logging_config)
    
    # prepare dtypes
    if dtypes is None:
        dtypes = [SUPPORTED_RAW_DATA_TYPES[0]]
    elif type(dtypes) is str:
        dtypes = [dtypes]
    # NOTE: if the data source supports only one raw data type, e.g. binance has only 'raw_tick', 
    # then 'raw' data type will be converted to 'raw_tick' implicitly
    dtypes = [SUPPORTED_RAW_DATA_TYPES[0] if dtype.lower() == 'raw' else dtype.lower() for dtype in dtypes]
    assert all(dtype in SUPPORTED_DATA_TYPES for dtype in dtypes), f'{dtypes=} but {SUPPORTED_DATA_TYPES=}'

    # prepare pdts
    if pdts is None:
        pdts = []
    elif type(pdts) is str:
        pdts = [pdts]
    pdts = [pdt.replace('-', '_').upper() for pdt in pdts]
    if ptypes is None:
        ptypes = []
    elif type(ptypes) is str:
        ptypes = [ptypes]
    ptypes = [ptype.upper() for ptype in ptypes]
    validate_pdts_and_ptypes(source, pdts, ptypes, is_cli=False)
    if not pdts:
        pdts = create_pdts_using_ptypes(ptypes)
    if not ptypes:
        ptypes = SUPPORTED_PRODUCT_TYPES[:]
    
    # prepare dates
    start_date = start_date or datetime.datetime.strptime(DATA_START_DATE, '%Y-%m-%d').date()
    end_date = end_date or datetime.datetime.now(tz=datetime.timezone.utc).date()
    dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
    
    # check if use Ray
    ray_tasks = defaultdict(list)
    cprint(f"Ray is {'enabled' if use_ray else 'disabled'}", style='bold')
    
    # check if use MinIO
    cprint(f"MinIO is {'enabled' if use_minio else 'disabled'}", style='bold')

    cprint(f'PFeed: downloading historical data from {source}, {start_date=} {end_date=}', style='bold yellow')
    for pdt in pdts if use_ray else tqdm(pdts, desc=f'Downloading {source} historical data by product', colour='green'):
        try:
            pdt_splits = pdt.split('_')
            ptype = pdt_splits[-1].upper()
            exchange = Exchange(env='LIVE', ptype=ptype)
            product = exchange.create_product(pdt)
        except KeyError:
            raise ValueError(f'"{pdt}" is not a valid product in {source}')
        efilenames = api.get_efilenames(pdt)
        # check if the efilename created by the date exists in the efilenames (files on the data server)
        dates = [date for date in dates if create_efilename(pdt, date) in efilenames]
        for date in dates if use_ray else tqdm(dates, desc=f'Downloading {source} {pdt} historical data by date', colour='yellow'):
            if use_ray:
                ray_tasks[pdt].append((product, date))
            else:
                run_etl(product, date, dtypes, use_minio)

    if use_ray:
        import ray
        from ray.util.queue import Queue

        logical_cpus = os.cpu_count()
        num_cpus = min(num_cpus, logical_cpus)
        ray.init(num_cpus=num_cpus)
        print(f"Ray's num_cpus is set to {num_cpus}")
        
        @ray.remote
        def _run_task(log_queue: Queue, product: BaseProduct, date: str):
            if not logger.handlers:
                logger.addHandler(QueueHandler(log_queue))
                logger.setLevel(logging.DEBUG)
            run_etl(product, date, dtypes, use_minio)

        batch_size = num_cpus
        log_queue = Queue()
        QueueListener(log_queue, *logger.handlers, respect_handler_level=True).start()
        for pdt in tqdm(ray_tasks, desc=f'Downloading {source} historical data by product', colour='green'):
            batches = [ray_tasks[pdt][i: i + batch_size] for i in range(0, len(ray_tasks[pdt]), batch_size)]
            for batch in tqdm(batches, desc=f'Downloading {source} {pdt} historical data by batch ({batch_size=})', colour='yellow'):
                futures = [_run_task.remote(log_queue, *task) for task in batch]
                ray.get(futures)
        
    logger.warning(f'finished downloading {source} historical data to {config.data_path}')
