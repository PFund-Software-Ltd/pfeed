# ruff: noqa
# /// script
# dependencies = [
#     "marimo==0.25.2",
#     "pfeed==0.0.23",
#     # pin the transitive deps too: micropip resolves `>=` at every page load, so they'd drift
#     # your packages: the download/retrieve path itself
#     "pfeed-bybit==0.0.2",
#     "pfund-bybit==0.0.3",
#     "pfund-kit==0.1.24",
#     "pfund-blocks==0.0.4",
#     # patched in the emscripten cell: internals they rely on can change in any release
#     "httpx2==2.16.0",
#     "rich==15.0.0",
#     "tqdm==4.70.1",
#     # validates the schema on download
#     "pandera==0.35.0",
#     # NOT pinned: compiled packages (polars, pyarrow, pydantic, msgspec) come from Pyodide's builds,
#     # which are fixed by the marimo version
# ]
# requires-python = ">=3.13"
#
# ///

import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell(hide_code=True)
async def _():
    import sys

    if sys.platform == "emscripten":
        import micropip

        await micropip.install(["polars", "pyarrow"])

        def _import_skipping_pyodide_preload(module: str):
            """Import `module` without marimo preloading Pyodide's own (older) build of it.

            marimo's WASM worker scans `import x` statements and loads Pyodide's build of x before
            micropip installs the notebook deps, so e.g. `import rich` would conflict with pfund-kit's rich>=15.
            A call with a string isn't an import statement, so the scanner skips it.
            """
            import importlib

            return importlib.import_module(module)

        ####################################################
        # marimo's WASM threading.local shim keeps instances in a WeakSet, so they must be hashable;
        # rich's ConsoleThreadLocals is a @dataclass (__hash__ = None), so switch it to identity semantics.
        _rich_console = _import_skipping_pyodide_preload("rich.console")
        _rich_console.ConsoleThreadLocals.__hash__ = object.__hash__
        _rich_console.ConsoleThreadLocals.__eq__ = object.__eq__

        ####################################################
        _tqdm = _import_skipping_pyodide_preload("tqdm")
        # avoid creating a thread in tqdm
        _tqdm.tqdm.monitor_interval = 0
        # tqdm's default lock includes a multiprocessing.RLock, which WASM doesn't support; use a thread lock instead
        import threading

        _tqdm.tqdm.set_lock(threading.RLock())

        ####################################################
        # CORS proxy shim: public.bybit.com sends no CORS headers,
        # so route every httpx2.get through a proxy that does.
        import httpx2

        _orig_get = httpx2.get
        PROXY = "https://pfeed-cors.lucky-water-569b.workers.dev/?url="

        def _proxied_get(url, *args, **kwargs):
            return _orig_get(PROXY + url, *args, **kwargs)

        httpx2.get = _proxied_get

        ####################################################
        # polars' to_arrow() overflows on wasm32 for strings > 12 bytes (e.g. "BTC_USDT_PERP"),
        # so let pyarrow build the string columns from Python lists instead
        import polars as pl
        import pyarrow as pa

        _orig_to_arrow = pl.DataFrame.to_arrow

        def _to_arrow(self, *args, **kwargs):
            str_cols = [col for col, dtype in self.schema.items() if dtype == pl.String]
            table = _orig_to_arrow(self.drop(str_cols), *args, **kwargs)
            for col in str_cols:
                table = table.append_column(
                    col, pa.array(self[col].to_list(), pa.large_string())
                )
            return table.select(self.columns)  # restore the original column order

        pl.DataFrame.to_arrow = _to_arrow

        ####################################################
        # Pyodide's polars has no native parquet scanner, and marimo's fallback only takes a single file,
        # but pfeed's ParquetIO.read passes a list, so read each file with pyarrow and concat
        import pyarrow.parquet as pq

        def _scan_parquet(source, *, schema=None, **kwargs):
            sources = source if isinstance(source, list) else [source]
            # pq.ParquetFile, not pq.read_table: read_table infers hive partitions from the path (e.g. resolution=1m/),
            # clashing with the file's own columns
            df = pl.concat(
                [pl.from_arrow(pq.ParquetFile(s).read()) for s in sources],
                how="diagonal_relaxed",
            )
            if (
                schema is not None
            ):  # pfeed passes the dataset schema, missing columns become nulls
                df = df.select(
                    pl.col(col).cast(dtype)
                    if col in df.columns
                    else pl.lit(None, dtype).alias(col)
                    for col, dtype in schema.items()
                )
            return df.lazy()

        pl.scan_parquet = _scan_parquet
    return


@app.cell
def _():
    import marimo as mo
    import pfeed as pe

    return mo, pe


@app.cell
def _(pe):
    bybit = pe.Bybit()
    feed = bybit.market_feed
    product = "BTC_USDT_PERP"  # or 'BTC_USDT_PERPETUAL', "ETH_USDT_CRYPTO"' etc.
    resolution = "1minute"  # or "1s"/"1second", "1t"/"1tick" etc.
    date = "2026-01-01"
    io = pe.ParquetIO()  # or pe.DuckLakeIO()
    return date, feed, io, product, resolution


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Download Data from Bybit

    > Bybit only supports market_feed, for some data sources, news_feed and other fundamental data will also be supported in the future
    """)
    return


@app.cell
def _(date, feed, io, product, resolution):
    download_result = feed.download(
        product=product,
        resolution=resolution,
        start_date=date,
        end_date=date,
        io=io,
    )
    df = download_result.data.collect()  # collect polars's LazyFrame
    df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Retrieve Data from Storage
    """)
    return


@app.cell
def _(date, feed, io, product, resolution):
    retrieve_result = feed.retrieve(
        product=product,
        resolution=resolution,
        start_date=date,
        end_date=date,
        io=io,
    )
    retrieved_df = retrieve_result.data.collect()
    retrieved_df
    return


if __name__ == "__main__":
    app.run()
