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
def _():
    import marimo as mo
    import pfeed as pe

    return (pe,)


@app.cell
def _(pe):
    print(pe.__version__)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
