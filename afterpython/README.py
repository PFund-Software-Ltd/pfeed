# ruff: noqa
# /// script
# dependencies = [
#     "marimo==0.25.2",
#     "pfeed[bybit]==0.0.22",
# ]
# requires-python = ">=3.13"
#
# ///

import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell
async def _():
    import sys

    if sys.platform == "emscripten":
        import micropip

        await micropip.install(["polars", "websockets", "pyarrow"])

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
        # similar to rich, avoid importing 
        _tqdm = _import_skipping_pyodide_preload("tqdm")

        # avoid creating a thread in tqdm
        _tqdm.tqdm.monitor_interval = 0

        ####################################################
        # CORS proxy shim: public.bybit.com sends no CORS headers,
        # so route every httpx2.get through a proxy that does.
        import httpx2

        _orig_get = httpx2.get
        PROXY = "https://pfeed-cors.lucky-water-569b.workers.dev/?url="

        def _proxied_get(url, *args, **kwargs):
            return _orig_get(PROXY + url, *args, **kwargs)

        httpx2.get = _proxied_get

    return


@app.cell(hide_code=True)
def _(mo):
    mo.callout(
        mo.md(
            "**Demo mode.** This is `pfeed` running in WASM — core features "
            "are not fully supported. "
            "For full functionality, install locally:\n\n"
            '```\npip install "pfeed[bybit,ray,prefect]"\n```\n'
            "Click **▶ Run all** (bottom right) to start. First run takes ~15s while Pyodide and dependencies load."
        ),
        kind="info",
    )
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


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
