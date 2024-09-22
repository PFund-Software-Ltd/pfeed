"""Downloads Bybit historical data"""
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_crypto import CryptoProduct
    from pfeed.types.common_literals import tSUPPORTED_STORAGES
    from pfeed.sources.bybit.types import tSUPPORTED_PRODUCT_TYPES, tSUPPORTED_DATA_TYPES
    from pfeed.resolution import ExtendedResolution
    
import os
import logging
import datetime
from collections import defaultdict
from logging.handlers import QueueHandler, QueueListener

from tqdm import tqdm
from rich.console import Console

from pfeed.datastore import check_if_minio_running
from pfeed.config_handler import get_config
from pfeed.sources.bybit import api
from pfeed.sources.bybit.const import (
    DATA_START_DATE, 
    DATA_SOURCE,
    SUPPORTED_PRODUCT_TYPES,
    SUPPORTED_DATA_TYPES,
    DTYPES_TO_RAW_RESOLUTIOS,
)
from pfeed.sources.bybit.utils import get_exchange, create_efilename, get_default_raw_resolution
from pfeed.utils.utils import get_dates_in_between
from pfeed.utils.validate import validate_pdts_and_ptypes


__all__ = ['download_historical_data']


def _convert_dtypes_to_resolutions(dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None) -> list[ExtendedResolution]:
    from pfeed.resolution import ExtendedResolution
    assert SUPPORTED_DATA_TYPES[0].startswith('raw_')
    default_raw_dtype = SUPPORTED_DATA_TYPES[0]
    if dtypes is None:
        dtypes = [default_raw_dtype]
    elif isinstance(dtypes, str):
        dtypes = [dtypes]
    # NOTE: if the data source supports only one raw data type, e.g. bybit has only 'raw_tick', 
    # then 'raw' data type will be converted to 'raw_tick' implicitly
    dtypes = [default_raw_dtype if dtype.lower() == 'raw' else dtype.lower() for dtype in dtypes]
    assert all(dtype in SUPPORTED_DATA_TYPES for dtype in dtypes), f'{dtypes=} but {SUPPORTED_DATA_TYPES=}'
    resolutions = [ExtendedResolution(
        DTYPES_TO_RAW_RESOLUTIOS[dtype] if dtype.startswith('raw_') else '1' + dtype[0]
    ) for dtype in dtypes]
    return resolutions


def _prepare_pdts(pdts: str | list[str] | None, ptypes: str | list[str] | None) -> tuple[list[str], list[str]]:
    def _create_pdts_using_ptypes() -> list[str]:
        exchange = get_exchange()
        for ptype in ptypes:
            category = exchange.PTYPE_TO_CATEGORY(ptype)
            epdts = api.get_epdts(ptype)
            # NOTE: if adapter(epdt, ref_key=category) == epdt, i.e. key is not found in pdt matching, meaning the product has been delisted
            pdts.extend([exchange.adapter(epdt, ref_key=category) for epdt in epdts if exchange.adapter(epdt, ref_key=category) != epdt])
    if pdts is None:
        pdts = []
    elif isinstance(pdts, str):
        pdts = [pdts]
    pdts = [pdt.replace('-', '_').upper() for pdt in pdts]
    if ptypes is None:
        ptypes = []
    elif isinstance(ptypes, str):
        ptypes = [ptypes]
    ptypes = [ptype.upper() for ptype in ptypes]
    validate_pdts_and_ptypes(DATA_SOURCE, pdts, ptypes, is_cli=False)
    if not pdts:
        if not ptypes:
            ptypes = SUPPORTED_PRODUCT_TYPES[:]
        pdts = _create_pdts_using_ptypes()
    return pdts


def _prepare_dates(start_date: str | None, end_date: str | None) -> tuple[datetime.date, datetime.date]:
    start_date = start_date or datetime.datetime.strptime(DATA_START_DATE, '%Y-%m-%d').date()
    end_date = end_date or datetime.datetime.now(tz=datetime.timezone.utc).date()
    return start_date, end_date


def _run_etl(storage: tSUPPORTED_STORAGES, product: CryptoProduct, date: datetime.date, resolutions: list[ExtendedResolution]):
    from pfeed import etl
    pdt = product.pdt
    if raw_data := api.get_data(pdt, date):
        raw_resolution = get_default_raw_resolution()
        for resolution in resolutions:
            data: bytes = etl.clean_raw_data(DATA_SOURCE, raw_data)
            data: bytes = etl.transform_data(DATA_SOURCE, pdt, data, raw_resolution, resolution)
            etl.load_data('BACKTEST', storage, DATA_SOURCE, data, pdt, resolution, date)
    else:
        raise Exception(f'failed to download {DATA_SOURCE} {pdt} {date} historical data')


def download_historical_data(
    products: str | list[str] | None=None, 
    dtypes: tSUPPORTED_DATA_TYPES | list[tSUPPORTED_DATA_TYPES] | None=None,
    ptypes: tSUPPORTED_PRODUCT_TYPES | list[tSUPPORTED_PRODUCT_TYPES] | None=None, 
    start_date: str | None=None,
    end_date: str | None=None,
    use_minio: bool=False,
    use_ray: bool=True,
    num_cpus: int=8,
) -> None:
    from pfund.plogging import set_up_loggers
    
    config = get_config()
    is_loggers_set_up = bool(logging.getLogger('pfeed').handlers)
    if not is_loggers_set_up:
        set_up_loggers(config.log_path, config.logging_config_file_path, user_logging_config=config.logging_config)
    logger = logging.getLogger(DATA_SOURCE.lower() + '_data')

    print(f'''Hint: 
        You can use the command "pfeed config --data-path {{your_path}}" to set your data path that stores downloaded data.
        The current data path is: {config.data_path}.
    ''')
    
    resolutions: list[ExtendedResolution] = _convert_dtypes_to_resolutions(dtypes)
    pdts = _prepare_pdts(products, ptypes)
    start_date, end_date = _prepare_dates(start_date, end_date)
    dates: list[datetime.date] = get_dates_in_between(start_date, end_date)
    
    Console().print(f"Ray is {'enabled' if use_ray else 'disabled'}", style='bold')
    Console().print(f"MinIO is {'enabled' if use_minio else 'disabled'}", style='bold')
    Console().print(f'PFeed: downloading historical data from {DATA_SOURCE}, {start_date=} {end_date=}', style='bold yellow')
    
    if use_minio:
        assert check_if_minio_running(), 'MinIO is not running or not detected on, please use "pfeed docker-compose up -d" to start MinIO'
    storage = 'minio' if use_minio else 'local'
    exchange = get_exchange()
    ray_tasks = defaultdict(list)
    for pdt in pdts if use_ray else tqdm(pdts, desc=f'Downloading {DATA_SOURCE} historical data by product', colour='green'):
        try:
            product = exchange.create_product(pdt)
        except KeyError:
            raise ValueError(f'"{pdt}" is not a valid product in {DATA_SOURCE}')
        efilenames = api.get_efilenames(pdt)
        # check if the efilename created by the date exists in the efilenames (files on the data server)
        dates = [date for date in dates if create_efilename(pdt, date) in efilenames]
        for date in dates if use_ray else tqdm(dates, desc=f'Downloading {DATA_SOURCE} {pdt} historical data by date', colour='yellow'):
            if use_ray:
                ray_tasks[pdt].append((product, date))
            else:
                _run_etl(storage, product, date, resolutions)

    if use_ray:
        import atexit
        import ray
        from ray.util.queue import Queue
        
        atexit.register(lambda: ray.shutdown())
        
        @ray.remote
        def _run_task(log_queue: Queue, product: CryptoProduct, date: datetime.date):
            try:
                if not logger.handlers:
                    logger.addHandler(QueueHandler(log_queue))
                    logger.setLevel(logging.DEBUG)
                logger.debug(f'cleaning {DATA_SOURCE} {product} {date} data')
                _run_etl(storage, product, date, resolutions)
                return True
            except Exception:
                logger.exception(f'error downloading {DATA_SOURCE} {product} {date}:')
                return False
        
        def _get_batch_size(total_items, num_cpu, target_batches_per_cpu=5):
            import math
            total_batches = num_cpu * target_batches_per_cpu
            return max(1, math.ceil(total_items / total_batches))
        
        try:
            log_listener = None
            logical_cpus = os.cpu_count()
            num_cpus = min(num_cpus, logical_cpus)
            ray.init(num_cpus=num_cpus)
            batch_size = num_cpus
            print(f"Ray's num_cpus is set to {num_cpus}")
            log_queue = Queue()
            log_listener = QueueListener(log_queue, *logger.handlers, respect_handler_level=True)
            log_listener.start()
            for pdt in tqdm(ray_tasks, desc=f'Downloading {DATA_SOURCE} historical data by product', colour='green'):
                batches = [ray_tasks[pdt][i: i + batch_size] for i in range(0, len(ray_tasks[pdt]), batch_size)]
                for batch in tqdm(batches, desc=f'Downloading {DATA_SOURCE} {pdt} historical data', colour='yellow'):
                    futures = [_run_task.remote(log_queue, *task) for task in batch]
                    results = ray.get(futures)
                    if not all(results):
                        logger.warning(f'some downloading failed in {pdt=}, check {logger.name}.log for details')
            logger.warning(f'finished downloading {DATA_SOURCE} historical data to {config.data_path}')
        except KeyboardInterrupt:
            print("KeyboardInterrupt received, stopping download...")
        except Exception:
            logger.exception(f'Error in downloading {DATA_SOURCE} historical data:')
        finally:
            if log_listener:
                log_listener.stop()
            ray.shutdown()
