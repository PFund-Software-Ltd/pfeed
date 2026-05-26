# ruff: noqa
# /// script
# dependencies = [
#     "marimo",
#     "pfeed==0.0.10",
# ]
# requires-python = ">=3.11"
# ///

import marimo

__generated_with = "0.23.8"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import pfeed as pe

    return mo, pe


@app.cell(hide_code=True)
async def _():
    import sys

    # monkey patching for WASM to work in this demo
    if sys.platform == "emscripten":
        import micropip

        await micropip.install(["polars", "pyarrow", "pyodide-http"])

        ####################################################
        # avoid creating a thread in tqdm
        import tqdm

        tqdm.tqdm.monitor_interval = 0

        ####################################################
        import pyodide_http

        pyodide_http.patch_all()

        # CORS proxy shim — public.bybit.com sends no CORS headers,
        # so route every httpx.get through a proxy that does.
        import httpx

        _orig_get = httpx.get
        PROXY = "https://pfeed-cors.lucky-water-569b.workers.dev/?url="

        def _proxied_get(url, *args, **kwargs):
            return _orig_get(PROXY + url, *args, **kwargs)

        httpx.get = _proxied_get

        ####################################################
        # Pyodide polars has no native parquet scanner; marimo's fallback
        # only handles a single source. Iterate and concat instead.
        import polars as pl
        import pfeed._io.parquet_io as _pio

        def _patched_read(self, file_paths, **io_kwargs):
            io_kwargs = io_kwargs or self._read_options
            non_empty = [
                str(p) for p in file_paths if self.exists(p) and not self.is_empty(p)
            ]
            if not non_empty:
                return None
            lfs = [
                pl.scan_parquet(p, storage_options=self._storage_options, **io_kwargs)
                for p in non_empty
            ]
            return lfs[0] if len(lfs) == 1 else pl.concat(lfs)

        _pio.ParquetIO.read = _patched_read
    return


@app.cell(hide_code=True)
def _(mo):
    mo.callout(
        mo.md(
            "**Demo mode.** This is `pfeed` running in WASM — core features "
            "are not fully supported. "
            "For full functionality, install locally:\n\n"
            "```\npip install pfeed[core]\n```\n"
            "Click **▶ Run all** (top right) to start. First run takes ~15s while Pyodide and dependencies load."
        ),
        kind="info",
    )
    return


@app.cell
def _(pe):
    bybit = pe.Bybit()
    feed = bybit.market_feed
    product = "BTC_USDT_PERP"  # or 'BTC_USDT_PERPETUAL', "ETH_USDT_SPOT"' etc.
    resolution = "1minute"  # or "1s"/"1second", "1t"/"1tick" etc.
    date = "2026-01-01"
    storage_config = pe.StorageConfig(storage="cache")  # store to cache
    return date, feed, product, resolution, storage_config


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Download Data from Bybit
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    > Bybit only supports market_feed, for some data sources, news_feed and other fundamental data will also be supported in the future
    """)
    return


@app.cell
def _(date, feed, product, resolution, storage_config):
    download_result = feed.download(
        product=product,
        resolution=resolution,
        start_date=date,
        end_date=date,
        storage_config=storage_config,
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
def _(date, feed, product, resolution, storage_config):
    retrieve_result = feed.retrieve(
        product=product,
        resolution=resolution,
        start_date=date,
        end_date=date,
        storage_config=storage_config,
    )
    retrieved_df = retrieve_result.data.collect()
    retrieved_df
    return


if __name__ == "__main__":
    app.run()
