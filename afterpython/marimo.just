# README.py is the marimo notebook for the project website's landing page, exported to WASM.
# These recipes are ONLY for testing the WASM locally (`just marimo wasm && just marimo serve`)
# before running the afterpython build; the real website export is done by `ap build`
# (to afterpython/_build/readme_py/), not by this file. Nothing here is used by the website.
# Output goes to the repo root build/ (gitignored), not _build/: `ap build` wipes afterpython/_build/
out := parent_directory(justfile_directory()) / "build" / "README_py"

# Export README.py to WASM, extra args go to marimo: `just marimo wasm --mode edit`
wasm *args:
    marimo export html-wasm --sandbox --mode run --show-code -f -o {{out}} {{args}} {{justfile_directory()}}/README.py

# Serve the WASM build and open it in the browser (it can't run from file://): `just marimo serve --port 9000`
[arg("port", long="port")]
serve port="8000":
    (sleep 1 && python3 -m webbrowser -t http://localhost:{{port}}) &
    python3 -m http.server {{port}} -d {{out}}
