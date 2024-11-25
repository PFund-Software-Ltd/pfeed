import importlib

import click

import pfeed as pe
from pfeed.const.enums import DataSource, DataType, ProductType, DataStorage, DataRawLevel
from pfeed.const.aliases import ALIASES


# add aliases to supported download data sources
SUPPORTED_DATA_SOURCES = [data_source.value for data_source in DataSource]
SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DATA_SOURCES + [k for k, v in ALIASES.items() if v in SUPPORTED_DATA_SOURCES]


@click.command()
@click.option('--data-source', '--source', '-d', required=True, type=click.Choice(SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED, case_sensitive=False), help='Data source')
@click.option('--products', '-p', 'products', multiple=True, default=[], help='List of trading products')
@click.option('--product-types', '--pt', 'ptypes', multiple=True, default=[], type=click.Choice(ProductType, case_sensitive=False), help='List of product types, e.g. STOCK = get all stocks')
@click.option('--data-type', '--dt', 'dtype', type=click.Choice(DataType, case_sensitive=False), help='Data type, e.g. "tick", "second". If not provided, the lowest resolution of the data source will be used')
@click.option('--start-date', '-s', type=click.DateTime(formats=["%Y-%m-%d"]), help='Start date in YYYY-MM-DD format')
@click.option('--end-date', '-e', type=click.DateTime(formats=["%Y-%m-%d"]), help='End date in YYYY-MM-DD format')
@click.option('--raw-level', '--rl', 'raw_level', default='normalized', type=click.Choice(DataRawLevel, case_sensitive=False), help='Data raw level, e.g. "original" (raw), "normalized" (standard names), or "cleaned" (standard columns only)')
@click.option('--storage', '--store', '--destination', 'storage', default='local', type=click.Choice(DataStorage, case_sensitive=False), help='Storage destination')
@click.option('--num-cpus', '-n', type=int, help="number of logical CPUs used in Ray")
@click.option('--no-ray', is_flag=True, help='if enabled, Ray will not be used')
@click.option('--use-prefect', is_flag=True, help='if enabled, Prefect will be used')
@click.option('--data-path', type=click.Path(exists=False), help='Path to store downloaded data')
@click.option('--env-file', 'env_file_path', type=click.Path(exists=True), help='Path to the .env file')
@click.option('--debug', is_flag=True, help='if enabled, debug mode will be enabled where logs at DEBUG level will be printed')
def download(data_source, products, ptypes, dtype, start_date, end_date, raw_level, storage, num_cpus, no_ray, use_prefect, data_path, env_file_path, debug):
    """Download historical data from a data source"""
    pe.configure(data_path=data_path, env_file_path=env_file_path, debug=debug)
    data_source = ALIASES.get(data_source, data_source)
    Feed = DataSource[data_source.upper()].feed_class
    feed = Feed(
        use_ray=not no_ray, 
        use_prefect=use_prefect,
        ray_kwargs={'num_cpus': num_cpus} if num_cpus else {},
    )
    feed.download_historical_data(
        products=products,
        product_types=ptypes,
        data_type=str(feed.data_source.highest_resolution.timeframe) if not dtype else dtype.value,
        start_date=start_date.date().strftime('%Y-%m-%d') if start_date else start_date,
        end_date=end_date.date().strftime('%Y-%m-%d') if end_date else end_date,
        raw_level=raw_level.value,
        storage=storage,
    )
