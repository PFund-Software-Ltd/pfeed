# FXMacroData

[FXMacroData](https://fxmacrodata.com/?utm_source=github&utm_medium=referral&utm_campaign=pfeed&utm_content=docs)
publishes macroeconomic announcements (inflation, policy rates, employment and
similar releases) from official sources for a range of currencies. PFeed's
integration downloads realised announcement records and keeps the API's own
columns, without mapping them onto a separate economic schema.

## Install

```bash
pip install pfeed
```

No extra is needed. Without an API key, only USD data is available, limited to
the most recent 90 days and delayed by 15 minutes. For other currencies and full
history, set an API key before creating the client:

```bash
export FXMACRODATA_API_KEY="your-api-key"
```

`FXMD_API_KEY` is also recognized, and the feed accepts `api_key` directly. The
key is sent in the `X-API-Key` header, never in the URL.

## Download announcements

```python
import pfeed as pe

feed = pe.FXMacroData().announcement_feed
announcements = feed.download(currency="USD", indicator="inflation")

print(announcements.collect())
print(feed.last_response_metadata.get("freemium_window"))
```

`pe.FXMD` is an alias for `pe.FXMacroData`. `download()` returns a polars
`LazyFrame` and accepts optional `start_date` and `end_date`. It fetches every
page of results; `limit` sets the page size, from 1 to 100 (the default).
Response metadata from the last page, including `freemium_window` and
`freemium_delay` when they apply, is kept in `feed.last_response_metadata`.

The `indicator` slug is the one used by the FXMacroData API, for example
`inflation` or `policy_rate`.

## Current scope

The integration covers historical announcement records only. Forward-looking
release calendars, FX rates and other FXMacroData endpoints are not exposed
here, and there is no live-streaming feed.

See the runnable [example script](../../examples/fxmacrodata.py).
