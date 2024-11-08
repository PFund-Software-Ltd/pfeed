import importlib

import click

import pfeed as pe
from pfeed.const.enums import DataSource, DataType, ProductType
from pfeed.const.aliases import ALIASES


# add aliases to supported download data sources
SUPPORTED_DATA_SOURCES = [data_source.value for data_source in DataSource]
SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DATA_SOURCES + [k for k, v in ALIASES.items() if v in SUPPORTED_DATA_SOURCES]


@click.command()
@click.option('--data-source', '-d', required=True, type=click.Choice(SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED, case_sensitive=False), help='Data source')
@click.option('--products', '-p', 'products', multiple=True, default=[], help='List of trading products')
@click.option('--dtypes', '--dt', 'dtypes', multiple=True, default=[], type=click.Choice(DataType, case_sensitive=False), help=f'{DataType=}. How to pass in multiple values: --dt tick --dt second')
@click.option('--ptypes', '--pt', 'ptypes', multiple=True, default=[], type=click.Choice(ProductType, case_sensitive=False), help='List of product types, e.g. STOCK = get all stocks')
@click.option('--start-date', '-s', type=click.DateTime(formats=["%Y-%m-%d"]), help='Start date in YYYY-MM-DD format')
@click.option('--end-date', '-e', type=click.DateTime(formats=["%Y-%m-%d"]), help='End date in YYYY-MM-DD format')
@click.option('--num-cpus', '-n', default=8, type=int, help="number of logical CPUs used for Ray's tasks")
@click.option('--use-minio', '-m', is_flag=True, help='if enabled, data will be loaded into Minio')
@click.option('--no-ray', is_flag=True, help='if enabled, Ray will not be used')
@click.option('--env-file', 'env_file_path', type=click.Path(exists=True), help='Path to the .env file')
@click.option('--debug', is_flag=True, help='if enabled, debug mode will be enabled where logs at DEBUG level will be printed')
def download(data_source, products, dtypes, ptypes, start_date, end_date, num_cpus, no_ray, use_minio, env_file_path, debug):
    """Download historical data from a data source"""
    pe.configure(env_file_path=env_file_path, debug=debug)
    data_source = ALIASES.get(data_source, data_source)
    download_historical_data = getattr(importlib.import_module(f'pfeed.sources.{data_source.lower()})', 'download'))
    download_historical_data(
        products=products,
        dtypes=dtypes,
        ptypes=ptypes,
        start_date=start_date.date().strftime('%Y-%m-%d') if start_date else start_date,
        end_date=end_date.date().strftime('%Y-%m-%d') if end_date else end_date,
        use_ray=not no_ray,
        num_cpus=num_cpus,
        use_minio=use_minio,
    )
