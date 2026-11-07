# data source plugins, each module runs in its own folder with its own pixi env
# e.g. `just bybit` opens a shell in pfeed-bybit's default env, `exit` goes back to pfeed
#      (don't `cd ../..`, it keeps pfeed-bybit's env active)
#      `just bybit test` runs pfeed-bybit's tests
mod? bybit 'sources/pfeed-bybit'
mod? hyperliquid 'sources/pfeed-hyperliquid'
mod? hl 'sources/pfeed-hyperliquid'
mod? pfund 'sources/pfeed-pfund'
mod? alpha 'sources/pfeed-alphafund'

# upgrade one package in uv.lock and pixi.lock, bypassing cached PyPI metadata
# (pixi has no per-package refresh, so its whole PyPI cache is cleaned to see a new release)
lock package:
    uv lock --refresh-package {{package}} --upgrade-package {{package}}
    pixi clean cache --pypi -y
    pixi update {{package}}
