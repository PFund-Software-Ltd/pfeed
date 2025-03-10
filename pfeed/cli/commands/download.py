import click

import pfeed as pe
from pfeed.enums import DataSource, DataStorage, DataLayer
from pfeed.const.aliases import ALIASES
from pfeed.cli.utils import parse_extra_args


# add aliases to supported download data sources
SUPPORTED_DATA_SOURCES = [data_source.value for data_source in DataSource]
SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DATA_SOURCES + [k for k, v in ALIASES.items() if v in SUPPORTED_DATA_SOURCES]


@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.pass_context
@click.option('--data-source', '--source', '-d', required=True, type=click.Choice(SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED, case_sensitive=False), help='Data source')
@click.option('--product', '-p', help='trading product, e.g. BTC_USDT_PERP')
@click.option('--resolution', '-r', help='Data resolution (e.g. "1m" for 1 minute data) or Data type (e.g. "tick"). If not provided, the lowest resolution of the data source will be used')
@click.option('--rollback-period', '--rb', help='Rollback period (e.g. "1w" for 1 week of data)')
@click.option('--start-date', '-s', type=click.DateTime(formats=["%Y-%m-%d"]), help='Start date in YYYY-MM-DD format')
@click.option('--end-date', '-e', type=click.DateTime(formats=["%Y-%m-%d"]), help='End date in YYYY-MM-DD format')
@click.option('--data-layer', '--layer', default='cleaned', type=click.Choice([level.name for level in DataLayer], case_sensitive=False), help='Data layer, e.g. "raw", "cleaned", or "curated"')
@click.option('--data-domain', '--domain', default='', type=str, help='Custom domain name to categorize data')
@click.option('--to-storage', '--storage', '--destination', default='local', type=click.Choice(DataStorage, case_sensitive=False), help='Storage destination')
@click.option('--no-ray', is_flag=True, help='if enabled, Ray will not be used')
@click.option('--use-prefect', is_flag=True, help='if enabled, Prefect will be used')
@click.option('--use-deltalake', is_flag=True, help='if enabled, Delta Lake will be used')
@click.option('--data-path', type=click.Path(exists=False), help='Path to store downloaded data')
@click.option('--env-file', 'env_file_path', type=click.Path(exists=True), help='Path to the .env file')
@click.option('--debug', is_flag=True, help='if enabled, debug mode will be enabled where logs at DEBUG level will be printed')
def download(ctx, data_source, product, resolution, rollback_period, start_date, end_date, data_layer, data_domain, to_storage, no_ray, use_prefect, use_deltalake, data_path, env_file_path, debug):
    """Download historical data from a data source"""
    pe.configure(data_path=data_path, env_file_path=env_file_path, debug=debug)
    data_source = ALIASES.get(data_source, data_source)
    kwargs = {}
    if resolution:
        kwargs['resolution'] = resolution
    if rollback_period:
        kwargs['rollback_period'] = rollback_period
    if start_date:
        kwargs['start_date'] = start_date.date().strftime('%Y-%m-%d')
    if end_date:
        kwargs['end_date'] = end_date.date().strftime('%Y-%m-%d')
    product_specs = parse_extra_args(ctx.args)
    for k, v in product_specs.items():
        kwargs[k] = v
    
    Feed = DataSource[data_source.upper()].feed_class
    feed = Feed(
        use_ray=not no_ray, 
        use_prefect=use_prefect,
        use_deltalake=use_deltalake,
    )
    feed.download(
        product=product,
        data_layer=data_layer,
        data_domain=data_domain,
        to_storage=to_storage,
        **kwargs,
    )
