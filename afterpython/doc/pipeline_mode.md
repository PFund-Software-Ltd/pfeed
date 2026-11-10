# Pipeline Mode

By default, every verb (`download()`, `retrieve()`, `stream()`, `replay()`) **runs right away** and returns a `RunResult`.
In **pipeline mode**, a verb only **queues** a request and returns the feed itself, so you can chain more steps onto it and run everything later with `run()`.

```python
import pfeed as pe

bybit = pe.Bybit(pipeline_mode=True)
feed = bybit.market_feed
```

| | Default mode | Pipeline mode |
| --- | --- | --- |
| A verb returns | `RunResult`, already run | the feed, nothing has run yet |
| `transform()` / `load()` | not available, the data is already stored | chained onto the request just queued |
| Multiple requests | one run per verb call | queued and run together by one `run()` |

---

## Building a Pipeline

Each verb queues a request; `transform()` and `load()` apply to **the request just queued**:

```python
result = (
    feed.download(
        product="BTC_USDT_PERP",
        resolution="1minute",
        start_date="2026-01-01",
        end_date="2026-01-31",
    )
    .transform(your_function)  # your own function, takes and returns a dataframe
    # load (store) the data TO the storage, not load it from there
    .load(pe.ParquetIO(), data_layer="CURATED")
    .run()
)
df = result.data.collect()
```

This is the ETL in its explicit form: `download()` is the **E**xtract, `transform()` the **T**ransform and `load()` the **L**oad.

- `load()` is optional. Without it, the data is stored in the `io` passed to the verb, or not stored if there is none.
- `CLEANED` data can be stored as `CURATED` after your own transformations, see [Medallion Architecture](concepts.md#medallion-architecture).

---

## Combining Calls into One Run

Pipeline mode is **lazy**: a verb doesn't run, it only queues a request, and the execution is **deferred** until you call `run()`. So you can build up as many requests as you like first, e.g. one per product in a `for` loop, and `run()` then executes all of them together:

```python
feed = pe.Bybit(pipeline_mode=True).market_feed

for product in ["BTC_USDT_PERP", "ETH_USDT_PERP"]:
    # lazy: queues one download request per product, nothing is downloaded yet
    feed.download(
        product=product,
        resolution="1m",
        start_date="2026-01-01",
        end_date="2026-01-31",
        io=pe.ParquetIO(),
    )

# executes the deferred requests, each request runs as its own dataflow
result = feed.run()
df = result.data.collect()  # the outputs of all dataflows, combined into one
result.success  # False if any dataflow failed
result.errors  # a failed dataflow doesn't stop the others, its error is recorded here
```

```{important}
Batch requests (`download()`, `retrieve()`) and streaming requests (`stream()`, `replay()`) **can't be mixed** in one pipeline, the feed raises a `ValueError` when you try.
```

For `stream()` and `replay()` in pipeline mode, see [Streaming and Replaying](streaming.md).
