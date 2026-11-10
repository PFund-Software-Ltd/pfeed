# Streaming and Replaying

## Streaming in Pipeline Mode

In [pipeline mode](pipeline_mode.md), `stream()` is lazy like every other verb: it only queues a stream, which starts when you run the feed. So you can queue one stream per product in a `for` loop and run them all together, see [Combining Calls into One Run](pipeline_mode.md#combining-calls-into-one-run).

```python
import pfeed as pe

feed = pe.Bybit(pipeline_mode=True).market_feed
products = ["BTC_USDT_PERP", "ETH_USDT_PERP"]
```

There are three ways to run the queued streams and get their messages:

### 1. Iterate with `async for`

Iterating over the feed runs all its queued streams and yields their messages:

```python
for product in products:
    # queues the stream, nothing runs yet
    feed.stream(product=product, resolution="1tick")

async for msg in feed:  # runs all the queued streams
    print(msg)
```

### 2. Run in an Event Loop with `run_async()`

Pass a `callback` to `stream()`, it is called with each message. A feed has **one** callback for all its streams, so pass the same function to every `stream()` call. Inside an event loop (e.g. Jupyter), run the streams with `await feed.run_async()`:

```python
def on_message(msg):
    print(msg)


for product in products:
    feed.stream(product=product, resolution="1tick", callback=on_message)

await feed.run_async()  # runs all the queued streams
```

### 3. Run in a Script with `run()`

Outside an event loop (e.g. a plain Python script), use `feed.run()` instead, it blocks until the streams end:

```python
for product in products:
    feed.stream(product=product, resolution="1tick", callback=on_message)

feed.run()  # runs all the queued streams
```

To store the streamed data, pass `io` to `stream()`, which must support appending, e.g. `io=pe.DuckLakeIO()` (currently the only supported one). `load()` is not supported after `stream()`, since a stream is always stored as `CLEANED` data.

---

## Replaying in Pipeline Mode

`replay()` plays stored data back as a stream, e.g. for backtesting. Queue and run it exactly like `stream()`, with any of the three ways above:

```python
for product in products:
    # queues the replay, nothing runs yet
    feed.replay(
        product=product,
        resolution="1m",
        start_date="2026-01-01",
        end_date="2026-01-31",
        io=pe.ParquetIO(),  # where the stored data is read from, then replayed message by message like stream()
        replay_pace=0,  # 0 (default) = as fast as possible, 1.0 = one message per second, None = real time
        callback=on_message,  # only needed for 2. and 3.
    )

# then run all the queued replays, pick ONE of the three ways:
# 1. iterate with async for
async for msg in feed:
    print(msg)

# 2. run in an event loop (e.g. Jupyter)
await feed.run_async()

# 3. run in a script
feed.run()
```

- `io` is where the data is replayed **from**; a replay stores nothing.
- Multiple replays are merged into one timeline: their messages are interleaved in the order they would have arrived live, e.g. BTC and ETH bars alternate by time, not all BTC first.

---

## Streaming with Ray

A stream is an ETL pipeline that runs on every message:

- **E**xtract: receive the message from the data source, e.g. via websocket. **Network-bound**.
- **T**ransform: parse and standardize it, plus your own `transform(your_function)`. **CPU-bound**.
- **L**oad: store it to the storage you passed as `io`. **Blocking**: the messages are buffered and written in batches, each write pauses everything else in the process.

Without Ray, the main process does all three for every message:

```text
main process:  message ──► E ──► T ──► L
```

All three share the same `asyncio` event loop, which can't run CPU-bound or blocking work in parallel. So while **T** or **L** is busy, **E** waits, and a slow **T** or **L** delays receiving the next messages.

With [Ray](integrations.md#integration-with-ray), the main process only does **E** and hands each message to a Ray worker, which does **T** and **L** in parallel with the other workers:

```text
                                ┌──► worker 1:  T ──► L
main process:  message ──► E ───┼──► worker 2:  T ──► L
                                └──► worker 3:  T ──► L
```

Ray moves **T** and **L** out of the event loop. The main process is left with network-bound work only, which `asyncio` handles well.

**When to use it**: when **T** can't keep up with the incoming messages, i.e. messages arrive faster than one process can transform them, so they pile up and get processed later and later. Typically this is your own heavy `transform(your_function)` running on many products. If you stream a few products with only `pfeed`'s default transformations, one process is enough, and Ray only adds overhead.

```python
# or engine.add_feed("BYBIT", data_category="MARKET_DATA", num_workers=4)
feed = pe.Bybit(pipeline_mode=True, num_workers=4).market_feed
```

- Each product's stream stays on **one worker**, so its messages stay in order and a stateful `your_function` keeps its state.
- Each worker holds a CPU for the whole stream, so the number of workers is capped by Ray's free CPUs.
- The feed's `callback` and `async for` get the messages **before** they are transformed, with or without Ray.

---

## The Data Engine

To run streams and replays from **multiple sources** together, use the `DataEngine`. `engine.add_feed()` creates a feed in pipeline mode for you, so you queue requests on it and let the engine run them:

```python
engine = pe.DataEngine()
bybit_feed = engine.add_feed("BYBIT", data_category="MARKET_DATA")
bybit_feed.stream(product="BTC_USDT_PERP", resolution="1tick")
```
