# data source plugins, each module runs in its own folder with its own pixi env
# e.g. `just bybit` opens a shell in pfeed-bybit's default env, `exit` goes back to pfeed
#      (don't `cd ../..`, it keeps pfeed-bybit's env active)
#      `just bybit test` runs pfeed-bybit's tests
mod bybit 'sources/pfeed-bybit'
