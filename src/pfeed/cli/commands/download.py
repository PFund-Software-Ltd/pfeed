"""`pfeed download SOURCE PRODUCT RESOLUTION`: downloads historical market data and stores it."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from pfeed.io.base_io import BaseIO
    from pfeed.market.feed import MarketFeed

import datetime
import importlib
from pathlib import Path

import click
from pfund_kit.cli.utils import cli_args_to_kwargs

from pfeed import registry

# imported only when the command runs, to keep the CLI startup light
_IO_CLASS_PATHS = {
    "parquet": "pfeed.io.parquet_io:ParquetIO",
    "ducklake": "pfeed.io.ducklake_io:DuckLakeIO",
}

# pfund and alphafund are special-case sources, not vendors to download data from
_DOWNLOADABLE_SOURCES = [
    source
    for source in registry.list_sources()
    if source.lower() not in ("pfund", "alphafund")
]


def _create_io(io_name: str, path: Path) -> BaseIO:
    module_name, class_name = _IO_CLASS_PATHS[io_name].split(":")
    io_class = getattr(importlib.import_module(module_name), class_name)
    # --path is the data root, i.e. pfeed's config data_path, NOT the IO's base_path:
    # the IO's base_path is derived from it as <path>/<IO's DEFAULT_DIR_NAME>, e.g. <path>/parquet
    return io_class(base_path=str(path / io_class.DEFAULT_DIR_NAME))


@click.command(
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True)
)
@click.argument(
    "source", type=click.Choice(_DOWNLOADABLE_SOURCES, case_sensitive=False)
)
@click.argument("product")
@click.argument("resolution")
@click.option(
    "--start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="First date (inclusive), YYYY-MM-DD",
)
@click.option(
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Last date (inclusive), YYYY-MM-DD, defaults to yesterday (UTC)",
)
@click.option(
    "--rollback",
    default="1d",
    show_default=True,
    help='How far to roll back from today (UTC) when --start is not given, e.g. "7d", "ytd" or "max"',
)
@click.option(
    "--layer",
    type=click.Choice(["raw", "cleaned"], case_sensitive=False),
    default="cleaned",
    show_default=True,
    help="Data layer to download and store the data in",
)
@click.option(
    "--io",
    "io_name",
    type=click.Choice(list(_IO_CLASS_PATHS), case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Where and how to store the data",
)
@click.option(
    "--path",
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Directory to store the data under (default: pfeed's data path)",
)
@click.option(
    "--origin",
    default="",
    help="Sub-label for data from the same source but different origins",
)
@click.pass_context
def download(
    ctx: click.Context,
    source: str,
    product: str,
    resolution: str,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    rollback: str,
    layer: str,
    io_name: str,
    path: Path | None,
    origin: str,
) -> None:
    """Download historical market data of PRODUCT at RESOLUTION from SOURCE and store it.

    Product specs are passed as extra options, e.g. `--expiration 2025-12-26` for futures,
    leave them out to get an error listing the required ones.

    \b
    Examples:
        pfeed download bybit BTC_USDT_PERP 1m --start 2025-01-01 --end 2025-01-31
        pfeed download bybit BTC_USDT_PERP 1t --rollback 7d --io ducklake
    """
    import pfeed as pe
    from pfeed.enums import DataCategory

    path = (path or ctx.obj["config"].data_path).expanduser()
    io = _create_io(io_name.lower(), path)
    product_specs = cli_args_to_kwargs(ctx.args)
    # only the market feed has download()
    feed = cast("MarketFeed", pe.get_feed(source, DataCategory.MARKET_DATA))
    result = feed.download(
        product,
        resolution,
        rollback_period=rollback,
        start_date=start.date() if start else None,
        end_date=end.date() if end else None,
        data_origin=origin,
        data_layer=layer,
        io=io,
        **product_specs,
    )
    num_rows = 0
    if result.data is not None:
        import polars as pl

        num_rows = result.data.select(pl.len()).collect().item()
    click.echo(
        f"Downloaded {num_rows} rows of {product} {resolution} from {source}, stored in {io_name} under {path}"
    )
    if result.failed:
        for error in result.errors:
            click.echo(f"  {type(error).__name__}: {error}", err=True)
        raise click.ClickException(
            f"{len(result.failed)} of {len(result.dataflows)} dataflow(s) failed"
        )
