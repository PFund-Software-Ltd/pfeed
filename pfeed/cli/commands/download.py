import importlib

import click
from dotenv import find_dotenv, load_dotenv

from pfeed.const.commons import ALIASES, SUPPORTED_DOWNLOAD_DATA_SOURCES, SUPPORTED_DATA_TYPES


# add aliases to supported download data sources
SUPPORTED_DOWNLOAD_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DOWNLOAD_DATA_SOURCES + [k for k, v in ALIASES.items() if v in SUPPORTED_DOWNLOAD_DATA_SOURCES]

# 'raw' data type is implicit since it doesn't have the timeframe specified, but still allow it for convenience
# since for data source like bybit, there's only one raw data type, 'raw_tick', i.e. 'raw' will be converted to 'raw_tick'
SUPPORTED_DATA_TYPES_IMPLICIT_RAW_ALLOWED = SUPPORTED_DATA_TYPES + ['raw']


@click.command()
@click.pass_context
@click.option('--env-file', 'env_file_path', type=click.Path(exists=True), help='Path to the .env file')
@click.option('--data-source', '-d', required=True, type=click.Choice(SUPPORTED_DOWNLOAD_DATA_SOURCES_ALIASES_INCLUDED, case_sensitive=False), help='Data source')
@click.option('--dtype', '--dt', 'dtypes', multiple=True, default=['raw'], type=click.Choice(SUPPORTED_DATA_TYPES_IMPLICIT_RAW_ALLOWED, case_sensitive=False), help=f'{SUPPORTED_DATA_TYPES=}. How to pass in multiple values: --dt raw --dt tick')
@click.option('--pdt', '-p', 'pdts', multiple=True, default=[], help='List of trading products')
@click.option('--ptype', '--pt', 'ptypes', multiple=True, default=[], help='List of product types, e.g. PERP = get all perpetuals')
@click.option('--start-date', '-s', type=click.DateTime(formats=["%Y-%m-%d"]), help='Start date in YYYY-MM-DD format')
@click.option('--end-date', '-e', type=click.DateTime(formats=["%Y-%m-%d"]), help='End date in YYYY-MM-DD format')
@click.option('--batch-size', default=8, type=int, help='batch size for Ray tasks')  # REVIEW
@click.option('--no-ray', is_flag=True, help='if enabled, Ray will not be used')
@click.option('--use-minio', is_flag=True, help='if enabled, data will be loaded into Minio')
@click.option('--debug', is_flag=True, help='if enabled, debug mode will be enabled where logs at DEBUG level will be printed')
def download(ctx, env_file_path, data_source, pdts, dtypes, ptypes, start_date, end_date, batch_size, no_ray, use_minio, debug):
    if not env_file_path:
        if env_file_path := find_dotenv(usecwd=True, raise_error_if_not_found=False):
            click.echo(f'.env file path is not specified, using env file in "{env_file_path}"')
        else:
            click.echo('.env file is not found')
    load_dotenv(env_file_path, override=True)
    
    if data_source in ALIASES:
        data_source = ALIASES[data_source]
        
    pipeline = importlib.import_module(f'pfeed.sources.{data_source.lower()}.download')
    pipeline.download_historical_data(
        pdts=pdts,
        dtypes=list(dtypes),
        ptypes=list(ptypes),
        start_date=start_date.date().strftime('%Y-%m-%d') if start_date else start_date,
        end_date=end_date.date().strftime('%Y-%m-%d') if end_date else end_date,
        batch_size=batch_size,
        use_ray=not no_ray,
        use_minio=use_minio,
        debug=debug,
        config=ctx.obj['config'],
    )
