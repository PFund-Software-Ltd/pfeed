"""`pfeed stream SOURCE PRODUCT RESOLUTION`: streams live market data, prints it and optionally stores it."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from pfund_blocks.typing import Message

    from pfeed.market.feed import MarketFeed

from pathlib import Path

import click
from pfund_kit.cli.utils import cli_args_to_kwargs

from pfeed.cli.commands.download import (
    _DOWNLOADABLE_SOURCES,
    _IO_CLASS_PATHS,
    _create_io,
)


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.argument(
    "source", type=click.Choice(_DOWNLOADABLE_SOURCES, case_sensitive=False)
)
@click.argument("product")
@click.argument("resolution")
@click.option(
    "--io",
    "io_name",
    type=click.Choice(list(_IO_CLASS_PATHS), case_sensitive=False),
    default=None,
    help="Where and how to store the streamed data, if not given, data is not stored",
)
@click.option(
    "--path",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory to store the data under (default: pfeed's data path), only used with --io",
)
@click.option(
    "--env",
    type=click.Choice(["paper", "live"], case_sensitive=False),
    default="live",
    show_default=True,
    help="Trading environment",
)
@click.option(
    "--origin",
    default="",
    help="Sub-label for data from the same source but different origins",
)
@click.option(
    "--flush-interval",
    type=float,
    default=100,
    show_default=True,
    help="Seconds between writes of the buffered data, only used with --io",
)
@click.option(
    "--incremental-bars",
    is_flag=True,
    help="Also store the updates of a bar before it closes, only used with --io",
)
@click.option("--quiet", is_flag=True, help="Do not print the streamed data")
@click.pass_context
def stream(
    ctx: click.Context,
    source: str,
    product: str,
    resolution: str,
    io_name: str | None,
    path: Path | None,
    env: str,
    origin: str,
    flush_interval: float,
    incremental_bars: bool,
    quiet: bool,
) -> None:
    """Stream live market data of PRODUCT at RESOLUTION from SOURCE, press Ctrl-C to stop.

    Product specs are passed as extra options, e.g. `--expiration 2025-12-26` for futures,
    leave them out to get an error listing the required ones.

    \b
    Examples:
        pfeed stream bybit BTC_USDT_PERP 1m
        pfeed stream bybit BTC_USDT_PERP 1t --io ducklake
        pfeed stream bybit BTC_USDT_PERP 1m --io parquet --path ~/data --quiet
    """
    import pfeed as pe
    from pfeed.enums import DataCategory

    io = None
    if io_name:
        path = (path or ctx.obj["config"].data_path).expanduser()
        io = _create_io(io_name.lower(), path)
    product_specs = cli_args_to_kwargs(ctx.args)

    def _print_message(message: Message) -> None:
        # NOTE: data is None for error messages, or when the data is parsed by Ray workers
        for item in message["data"] or []:
            click.echo(
                f"{message['received_at']} {message['product']} {message['resolution']} {item}"
            )

    # only the market feed has stream()
    feed = cast("MarketFeed", pe.get_feed(source, DataCategory.MARKET_DATA))
    stored = f", stored in {io_name} under {path}" if io else ""
    try:
        result = feed.stream(
            product,
            resolution,
            data_origin=origin,
            io=io,
            env=cast('Literal["PAPER", "LIVE"]', env.upper()),
            callback=None if quiet else _print_message,
            store_incremental_bars=incremental_bars,
            flush_interval=flush_interval,
            **product_specs,
        )
    except KeyboardInterrupt:
        # NOTE: only reached if Ctrl-C comes before the streams start,
        # once they run, stream() catches the cancellation, flushes the buffered data to io and returns
        click.echo(f"Stopped streaming {product} {resolution} from {source}")
        return
    click.echo(f"Stopped streaming {product} {resolution} from {source}{stored}")
    if result.failed:
        for error in result.errors:
            click.echo(f"  {type(error).__name__}: {error}", err=True)
        raise click.ClickException(
            f"{len(result.failed)} of {len(result.dataflows)} dataflow(s) failed"
        )
