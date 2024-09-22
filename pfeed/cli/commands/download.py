import importlib

import click

import pfeed as pe
from pfeed.const.common import (
    ALIASES, 
    SUPPORTED_DOWNLOAD_DATA_SOURCES, 
    SUPPORTED_DATA_TYPES,
    SUPPORTED_PRODUCT_TYPES,
)


# add aliases to supported download data sources
SUPPORTED_DOWNLOAD_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DOWNLOAD_DATA_SOURCES + [k for k, v in ALIASES.items() if v in SUPPORTED_DOWNLOAD_DATA_SOURCES]

# 'raw' data type is implicit since it doesn't have the timeframe specified, but still allow it for convenience
# since for data source like bybit, there's only one raw data type, 'raw_tick', i.e. 'raw' will be converted to 'raw_tick'
SUPPORTED_DATA_TYPES_IMPLICIT_RAW_ALLOWED = SUPPORTED_DATA_TYPES + ['raw']


@click.command()
@click.option('--data-source', '-d', required=True, type=click.Choice(SUPPORTED_DOWNLOAD_DATA_SOURCES_ALIASES_INCLUDED, case_sensitive=False), help='Data source')
@click.option('--products', '-p', 'products', multiple=True, default=[], help='List of trading products')
@click.option('--dtypes', '--dt', 'dtypes', multiple=True, default=['raw'], type=click.Choice(SUPPORTED_DATA_TYPES_IMPLICIT_RAW_ALLOWED, case_sensitive=False), help=f'{SUPPORTED_DATA_TYPES=}. How to pass in multiple values: --dt raw --dt tick')
@click.option('--ptypes', '--pt', 'ptypes', multiple=True, default=[], type=click.Choice(SUPPORTED_PRODUCT_TYPES, case_sensitive=False), help='List of product types, e.g. PERP = get all perpetuals')
@click.option('--start-date', '-s', type=click.DateTime(formats=["%Y-%m-%d"]), help='Start date in YYYY-MM-DD format')
@click.option('--end-date', '-e', type=click.DateTime(formats=["%Y-%m-%d"]), help='End date in YYYY-MM-DD format')
@click.option('--num-cpus', '-n', default=8, type=int, help="number of logical CPUs used for Ray's tasks")
@click.option('--use-minio', '-m', is_flag=True, help='if enabled, data will be loaded into Minio')
@click.option('--no-ray', is_flag=True, help='if enabled, Ray will not be used')
@click.option('--env-file', 'env_file_path', type=click.Path(exists=True), help='Path to the .env file')
@click.option('--debug', is_flag=True, help='if enabled, debug mode will be enabled where logs at DEBUG level will be printed')
def download(data_source, products, dtypes, ptypes, start_date, end_date, num_cpus, no_ray, use_minio, env_file_path, debug):
    pe.configure(env_file_path=env_file_path, debug=debug)
    data_source = ALIASES.get(data_source, data_source)
    pipeline = importlib.import_module(f'pfeed.sources.{data_source.lower()}.download')
    pipeline.download_historical_data(
        products=products,
        dtypes=dtypes,
        ptypes=ptypes,
        start_date=start_date.date().strftime('%Y-%m-%d') if start_date else start_date,
        end_date=end_date.date().strftime('%Y-%m-%d') if end_date else end_date,
        use_ray=not no_ray,
        num_cpus=num_cpus,
        use_minio=use_minio,
    )
