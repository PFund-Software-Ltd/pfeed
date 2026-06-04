import datetime

import click
from pfund_kit.cli.utils import cli_args_to_kwargs

import pfeed as pe
from pfeed.enums import DataLayer, DataSource, DataStorage

# add aliases to supported streaming data sources
SUPPORTED_DATA_SOURCES = [data_source.value for data_source in DataSource]
SUPPORTED_DATA_SOURCES_ALIASES_INCLUDED = SUPPORTED_DATA_SOURCES + [
    pe.alias(ds) for ds in SUPPORTED_DATA_SOURCES if pe.alias(ds)
]

# streaming is only supported by market feeds, so unlike `download` there is no
# --category flag — the command always targets the source's market feed.
SUPPORTED_ENVS = ["BACKTEST", "PAPER", "LIVE"]


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
    "--product", "-p", required=True, help="trading product, e.g. BTC_USDT_PERP"
)
@click.option(
    "--resolution",
    "-r",
    required=True,
    help='Data resolution (e.g. "1m" for 1 minute data) or data type (e.g. "tick")',
)
@click.option(
    "--symbol",
    help="Source-specific symbol. If omitted, it is derived from --product (the derivation may be wrong, in which case pass it explicitly)",
)
@click.option(
    "--env",
    default="LIVE",
    type=click.Choice(SUPPORTED_ENVS, case_sensitive=False),
    help="Trading environment. LIVE connects to the live source via websocket; BACKTEST replays historical data from storage",
)
@click.option(
    "--rollback-period",
    "--rb",
    help='Rollback period from today (e.g. "1w", "ytd", or "max"), only used in BACKTEST replay when --start-date is not given',
)
@click.option(
    "--start-date",
    "-s",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date in YYYY-MM-DD format (BACKTEST replay range)",
)
@click.option(
    "--end-date",
    "-e",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date in YYYY-MM-DD format (BACKTEST replay range)",
)
@click.option(
    "--replay-pace",
    type=float,
    default=None,
    help="BACKTEST only. Pacing between row emissions in seconds. Omit or 0 = ASAP (default), >0 = fixed cadence in seconds",
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
    help='Data layer, e.g. "RAW", "CLEANED", or "CURATED". Only used when --to-storage is set (LIVE) or as the replay source layer (BACKTEST)',
)
@click.option(
    "--to-storage",
    "--storage",
    "--destination",
    default=None,
    type=click.Choice([storage.value for storage in DataStorage], case_sensitive=False),
    help="LIVE: where to persist streamed data (omit to not persist). BACKTEST: where to read replay data from (defaults to LOCAL)",
)
@click.option(
    "--data-path",
    type=click.Path(exists=False),
    help="Path to store/read streamed data",
)
@click.option(
    "--clean-data/--no-clean-data",
    default=True,
    help="LIVE only. Whether to clean (normalize/standardize/resample) raw streamed data. Ignored in BACKTEST replay (replayed data is already cleaned)",
)
@click.option(
    "--no-print",
    is_flag=True,
    help="Do not echo incoming messages to the terminal",
)
@click.option(
    "--debug",
    is_flag=True,
    help="if enabled, debug mode will be enabled where logs at DEBUG level will be printed",
)
def stream(
    ctx: click.Context,
    data_source: str,
    product: str,
    resolution: str,
    symbol: str | None,
    env: str,
    rollback_period: str | None,
    start_date: datetime.datetime | None,
    end_date: datetime.datetime | None,
    replay_pace: float | None,
    data_origin: str,
    data_layer: str,
    to_storage: str | None,
    data_path: str | None,
    clean_data: bool,
    no_print: bool,
    debug: bool,
):
    """Stream market data live from a data source, or replay it from storage (BACKTEST)"""
    from pfeed.feeds import create_market_feed
    from pfeed.storages.storage_config import StorageConfig

    pe.configure(data_path=data_path)
    if debug:
        pe.configure_logging(debug=debug)
    data_source = pe.alias.resolve(data_source)
    env = env.upper()
    is_replaying = env == "BACKTEST"

    # LIVE: storage_config = where to persist (None ⇒ not persisted).
    # BACKTEST: storage_config = where to read replay data FROM, so it must exist.
    storage_config = None
    if to_storage or is_replaying:
        storage_config = StorageConfig(
            storage=to_storage or DataStorage.LOCAL,
            data_layer=data_layer,
            # data_path=None lets StorageConfig fall back to the configured data_path above
            data_path=data_path,
        )

    def _print_message(websocket_name: str, msg: dict) -> None:
        click.echo(f"[{websocket_name}] {msg}")

    kwargs = {
        "product": product,
        "resolution": resolution,
        "env": env,
        "data_origin": data_origin,
        "storage_config": storage_config,
        "clean_data": clean_data,
        "callback": None if no_print else _print_message,
    }
    if symbol:
        kwargs["symbol"] = symbol
    if rollback_period:
        kwargs["rollback_period"] = rollback_period
    if start_date:
        kwargs["start_date"] = start_date.date().strftime("%Y-%m-%d")
    if end_date:
        kwargs["end_date"] = end_date.date().strftime("%Y-%m-%d")
    if replay_pace is not None:
        kwargs["replay_pace"] = replay_pace
    # extra data-source-specific product specs, e.g. --strike-price 10000
    product_specs = cli_args_to_kwargs(ctx.args)
    for k, v in product_specs.items():
        kwargs[k] = v

    # feed.stream() handles both live websocket (LIVE) and historical replay
    # (BACKTEST, incl. replay_pace) natively — no manual replay loop needed here.
    feed = create_market_feed(data_source=data_source)
    feed.stream(**kwargs)
