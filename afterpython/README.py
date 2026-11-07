# ruff: noqa
# /// script
# dependencies = [
#     "marimo==0.25.2",
#     "pfeed==0.0.21",
# ]
# requires-python = ">=3.13"
#
# ///

import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell
def _():
    import sys

    if sys.platform == "emscripten":
        # marimo's WASM threading.local shim keeps instances in a WeakSet, so they must be hashable;
        # rich's ConsoleThreadLocals is a @dataclass (__hash__ = None), so switch it to identity semantics.
        # importlib, not `import rich.console`: marimo would preload Pyodide's older rich, conflicting with pfund-kit's rich>=15
        import importlib

        _rich_console = importlib.import_module("rich.console")
        _rich_console.ConsoleThreadLocals.__hash__ = object.__hash__
        _rich_console.ConsoleThreadLocals.__eq__ = object.__eq__

    import marimo as mo
    import pfeed as pe

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
