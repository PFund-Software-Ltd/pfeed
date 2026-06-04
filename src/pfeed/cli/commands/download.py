import datetime

import click
from pfund_kit.cli.utils import cli_args_to_kwargs

import pfeed as pe
from pfeed.enums import DataCategory, DataLayer, DataSource, DataStorage

# add aliases to supported download data sources
SUPPORTED_DATA_SOURCES = [data_source.value for data_source in DataSource]
SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DATA_SOURCES + [
    pe.alias(ds) for ds in SUPPORTED_DATA_SOURCES if pe.alias(ds)
]

# a data source exposes one feed per data category; "download" picks the feed of
# the requested category via pfeed.feeds.create_feed. Not every category has a
# downloadable feed — create_feed raises a clear error if it doesn't.
SUPPORTED_CATEGORIES = [category.value for category in DataCategory]


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.pass_context
@click.option(
    "--data-source",
    "--source",
    "-d",
    required=True,
    type=click.Choice(SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED, case_sensitive=False),
    help="Data source",
)
@click.option(
    "--category",
    "-c",
    default=DataCategory.MARKET_DATA.value,
    type=click.Choice(SUPPORTED_CATEGORIES, case_sensitive=False),
    help="Which feed of the data source to download from (a source has one feed per data category)",
)
@click.option("--product", "-p", help="trading product, e.g. BTC_USDT_PERP")
@click.option(
    "--symbol",
    help="Source-specific symbol. If omitted, it is derived from --product (the derivation may be wrong, in which case pass it explicitly)",
)
@click.option(
    "--resolution",
    "-r",
    help=f'Data resolution (e.g. "1m" for 1 minute data) or data type (e.g. "tick"). Required for --category {DataCategory.MARKET_DATA.value}',
)
@click.option(
    "--rollback-period",
    "--rb",
    help='Rollback period from today (e.g. "1w" for 1 week of data, "ytd", or "max"), only used when --start-date is not given',
)
@click.option(
    "--start-date",
    "-s",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date in YYYY-MM-DD format",
)
@click.option(
    "--end-date",
    "-e",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date in YYYY-MM-DD format",
)
@click.option(
    "--data-origin",
    default="",
    help="Origin label used to distinguish data from different providers of the same source",
)
@click.option(
    "--data-layer",
    "--layer",
    default="CLEANED",
    type=click.Choice([layer.name for layer in DataLayer], case_sensitive=False),
    help='Data layer, e.g. "RAW", "CLEANED", or "CURATED"',
)
@click.option(
    "--to-storage",
    "--storage",
    "--destination",
    default="LOCAL",
    type=click.Choice([storage.value for storage in DataStorage], case_sensitive=False),
    help="Storage destination to persist downloaded data to",
)
@click.option(
    "--data-path", type=click.Path(exists=False), help="Path to store downloaded data"
)
@click.option(
    "--debug",
    is_flag=True,
    help="if enabled, debug mode will be enabled where logs at DEBUG level will be printed",
)
def download(
    ctx: click.Context,
    data_source: str,
    category: str,
    product: str | None,
    symbol: str | None,
    resolution: str | None,
    rollback_period: str | None,
    start_date: datetime.datetime | None,
    end_date: datetime.datetime | None,
    data_origin: str,
    data_layer: str,
    to_storage: str,
    data_path: str | None,
    debug: bool,
):
    """Download historical data from a data source"""
    from pfeed.feeds import create_feed
    from pfeed.storages.storage_config import StorageConfig

    pe.configure(data_path=data_path)
    if debug:
        pe.configure_logging(debug=debug)
    data_source = pe.alias.resolve(data_source)
    data_category = DataCategory[category.upper()]
    is_market = data_category == DataCategory.MARKET_DATA

    if is_market and not resolution:
        raise click.UsageError(
            f"--resolution is required for --category {DataCategory.MARKET_DATA.value}"
        )
    if is_market and not product:
        raise click.UsageError(
            f"--product is required for --category {DataCategory.MARKET_DATA.value}"
        )

    # always build a StorageConfig: download() persists nothing when it is None.
    storage_config = StorageConfig(
        storage=to_storage,
        data_layer=data_layer,
        # data_path=None lets StorageConfig fall back to the configured data_path above
        data_path=data_path,
    )

    kwargs = {
        "product": product,
        "data_origin": data_origin,
        "storage_config": storage_config,
    }
    if is_market:
        kwargs["resolution"] = resolution
    if symbol:
        kwargs["symbol"] = symbol
    if rollback_period:
        kwargs["rollback_period"] = rollback_period
    if start_date:
        kwargs["start_date"] = start_date.date().strftime("%Y-%m-%d")
    if end_date:
        kwargs["end_date"] = end_date.date().strftime("%Y-%m-%d")
    # extra data-source-specific product specs, e.g. --strike-price 10000
    product_specs = cli_args_to_kwargs(ctx.args)
    for k, v in product_specs.items():
        kwargs[k] = v

    feed = create_feed(data_source=data_source, data_category=data_category)
    feed.download(**kwargs)
