# ruff: noqa
# /// script
# dependencies = [
#     "marimo==0.25.2",
#     "pfeed==0.0.20",
# ]
# requires-python = ">=3.13"
#
# [tool.uv.sources]
# pfeed = { path = "../dist/pfeed-0.0.20-py3-none-any.whl" }
# ///

import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import pfeed as pe

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
