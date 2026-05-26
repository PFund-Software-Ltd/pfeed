# ruff: noqa
# /// script
# dependencies = [
#     "marimo",
#     "pfeed==0.0.8",
# ]
# requires-python = ">=3.11"
# ///

import marimo

__generated_with = "0.23.8"
app = marimo.App()


@app.cell
async def _():
    import marimo as mo
    import pfeed as pe
    import sys

    if sys.platform == "emscripten":
        import micropip

        await micropip.install(["polars", "pyarrow", "pyodide-http"])

        # avoid creating a thread in tqdm
        import tqdm

        tqdm.tqdm.monitor_interval = 0

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
    return (pe,)


@app.cell
def _(pe):
    bybit = pe.Bybit()
    feed = bybit.market_feed
    result = feed.download(
        product="BTC_USDT_PERP",
        resolution="1minute",
        start_date="2026-01-01",
        end_date="2026-01-01",
        storage_config=None,
    )
    df = result.data.collect()
    df
    return


if __name__ == "__main__":
    app.run()
