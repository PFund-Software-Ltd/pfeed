import os
import importlib

import click
from dotenv import find_dotenv, load_dotenv

from pfeed.utils.validate import validate_pdts_and_ptypes
from pfeed.const.commons import ALIASES, SUPPORTED_DOWNLOAD_DATA_SOURCES, SUPPORTED_DATA_TYPES


# add aliases to supported download data sources
SUPPORTED_DOWNLOAD_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DOWNLOAD_DATA_SOURCES + [k for k, v in ALIASES.items() if v in SUPPORTED_DOWNLOAD_DATA_SOURCES]


@click.command()
@click.pass_context
@click.option('--env-file', 'env_file_path', type=click.Path(exists=True), help='Path to the .env file')
@click.option('-s', '--source', required=True, type=click.Choice(SUPPORTED_DOWNLOAD_DATA_SOURCES_ALIASES_INCLUDED, case_sensitive=False), help='Data source')
@click.option('-p', '--pdts', multiple=True, default=[], help='List of trading products')
@click.option('--dtypes', '--dt', multiple=True, type=click.Choice(SUPPORTED_DATA_TYPES, case_sensitive=False), help='List of data types, e.g. raw, tick, second, minute, hour, daily')
@click.option('--ptypes', '--pt', multiple=True, default=[], help='List of product types, e.g. PERP = get all perpetuals')
@click.option('-b', '--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), help='Start date in YYYY-MM-DD format')
@click.option('-n', '--end-date', type=click.DateTime(formats=["%Y-%m-%d"]), help='End date in YYYY-MM-DD format')
@click.option('--batch-size', default=8, type=int, help='batch size for Ray tasks')  # REVIEW
@click.option('--no-ray', is_flag=True)
@click.option('--use-minio', is_flag=True)
@click.option('--debug', is_flag=True)
def download(ctx, env_file_path, source, pdts, dtypes, ptypes, start_date, end_date, batch_size, no_ray, use_minio, debug):
    if not env_file_path:
        env_file_path = find_dotenv(usecwd=True, raise_error_if_not_found=True)
        click.echo(f'.env file path is not specified, using env file in "{env_file_path}"')
    load_dotenv(env_file_path, override=True)
    
    if source in ALIASES:
        source = ALIASES[source]
    pdts = [pdt.replace('-', '_') for pdt in pdts]
    validate_pdts_and_ptypes(source, pdts, ptypes, is_cli=True)
    if start_date:
        start_date = start_date.date()
    if end_date:
        end_date = end_date.date()
    pipeline = importlib.import_module(f'pfeed.sources.{source.lower()}.download')
    pipeline.run(
        pdts=list(pdts),
        dtypes=list(dtypes),
        ptypes=list(ptypes),
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
        use_ray=not no_ray,
        use_minio=use_minio,
        debug=debug,
        config=ctx.obj['config'],
    )
