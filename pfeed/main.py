import os
import sys
import logging
import importlib
from types import TracebackType
from argparse import ArgumentParser


from pfeed.const.paths import LOG_PATH, DATA_PATH


ALIASES = {
    'yf': 'yahoo_finance',
}


def _custom_excepthook(exception_class: type[BaseException], exception: BaseException, traceback: TracebackType):
    '''Catches any uncaught exceptions and logs them'''
    # sys.__excepthook__(exception_class, exception, traceback)
    try:
        raise exception
    except:
        logging.getLogger('pfeed').exception('Uncaught exception:')


if __name__ == '__main__':
    sys.excepthook = _custom_excepthook
    
    parser = ArgumentParser()
    parser.add_argument('-e', '--env', choices=['PAPER', 'LIVE'], default='LIVE', required=False)
    parser.add_argument('--data-path', dest='data_path', default=str(DATA_PATH), required=False)
    parser.add_argument('--log-path', dest='log_path', default=str(LOG_PATH), required=False)
    parser.add_argument('-m', '--mode', default='historical', help='historical=historical data processing; streaming=live data streaming', choices=['historical', 'streaming'], required=False)
    parser.add_argument('-b', '--start-date', dest='start_date', help='Start date in YYYY-MM-DD format', required=False)
    parser.add_argument('-n', '--end-date', dest='end_date', help='End date in YYYY-MM-DD format', required=False)
    parser.add_argument('--ptypes', nargs='+', help='List of product types, e.g. PERP = get all perpetuals', required=False)
    parser.add_argument('-p', '--pdts', nargs='+', help='List of trading products', required=False)
    parser.add_argument('--dtypes', nargs='+', help='List of data types, e.g. raw, tick, second, minute, hour, daily', required=False)
    parser.add_argument(
        '-s', '--source', required=True,
        choices=[
            'bybit',
        ],
    )
    parser.add_argument('-z', '--batch-size', dest='batch_size', default=8, help='batch size for Ray tasks', type=int, required=False)
    parser.add_argument('--no-ray', dest='no_ray', action='store_true', required=False)
    parser.add_argument('--no-minio', dest='no_minio', action='store_true', required=False)
    
    # handle arguments
    args = parser.parse_args()
    print("Ray is disabled") if args.no_ray else print("Ray is enabled")
    print("MinIO is disabled") if args.no_minio or os.getenv('MINIO_ENDPOINT') is None else print("MinIO is enabled")
    env, source, mode = args.env.upper(), args.source.lower(), args.mode.lower()

    source = ALIASES.get(source, source)
    pipeline = importlib.import_module(f'pfeed.sources.{source}.{mode}')
    if mode == 'historical':
        pipeline.run(
            dtypes=args.dtypes,
            ptypes=args.ptypes,
            pdts=args.pdts,
            start_date=args.start_date,
            end_date=args.end_date,
            log_path=args.log_path,
            data_path=args.data_path,
            batch_size=args.batch_size,
            use_ray=not args.no_ray,
            use_minio=not args.no_minio,
        )
    else:
        # TODO: implement streaming mode
        raise NotImplementedError(f'{mode} is not implemented yet')