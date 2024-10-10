# Notes on using `myst` to execute ipynb files

`myst start`: reads the outputs (if any) of ipynb files but won't execute them.

`myst start --execute`: reads the outputs from _build/execute, if not exist, it will execute the ipynb files and create the outputs.

If you want to rebuild the ipynb outputs, you need to run `myst clean --execute` to delete the _build/execute folder, then run `myst start --execute` again.

---

The same logic applies to `myst build`.
1. `myst clean --execute` (if needed, for a clean build, run `myst clean` to clear cache)
2. `myst build --execute --html`
3. `npx serve _build/html`

