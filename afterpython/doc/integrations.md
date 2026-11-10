# Integrations

## Integration with Ray

[Ray](https://www.ray.io/) runs your dataflows **in parallel**, e.g. downloading many products or a long date range at once.

```bash
pip install "pfeed[ray]"
```

Pass `num_workers` to the source, and each dataflow runs as a Ray task:

```python
import pfeed as pe

bybit = pe.Bybit(pipeline_mode=True, num_workers=4)
feed = bybit.market_feed

for product in ["BTC_USDT_PERP", "ETH_USDT_PERP", "SOL_USDT_PERP"]:
    feed.download(
        product=product,
        resolution="1m",
        start_date="2026-01-01",
        end_date="2026-01-31",
        io=pe.ParquetIO(),
    )

result = feed.run()
```

- Ray is initialized for you if `ray.init()` hasn't been called yet. To configure it (e.g. `num_cpus`, or connecting to a cluster), call `ray.init()` yourself first.
- `num_workers` isn't capped by your local CPU count, since Ray may run on a cluster and queue the tasks it can't schedule yet.
- Each feed can have its own number of workers, e.g. `pe.Bybit(num_workers={"market_data": 4})`, or `feed.set_num_workers(4)` before calling any verb.

```{note}
The IO the data is stored in must support concurrent writes from Ray workers. Both `ParquetIO` and `DuckLakeIO` do.
```

---

## Integration with Prefect

[Prefect](https://www.prefect.io/) gives your dataflows a **UI, logs and schedules**. `pfeed` already handles correctness itself (e.g. retries of the source's API), so Prefect is an optional ops layer on top.

```bash
pip install "pfeed[prefect]"
```

Run the feed with `use_prefect=True`, and each dataflow runs as a Prefect flow, with its extract, transform and load steps as tasks:

```python
import pfeed as pe

feed = pe.Bybit(pipeline_mode=True).market_feed

result = (
    feed.download(
        product="BTC_USDT_PERP",
        resolution="1m",
        start_date="2026-01-01",
        end_date="2026-01-31",
        io=pe.ParquetIO(),
    ).run(use_prefect=True, retries=2)  # extra kwargs go to Prefect's @flow decorator
)
```

- `use_prefect` is an argument of `run()`, so it needs [pipeline mode](pipeline_mode.md).
- Prefect only runs batch dataflows, i.e. `download()` and `retrieve()`.
- The source's logs (e.g. `pfeed.bybit`) show up in the Prefect UI.
- Ray and Prefect can be combined: if `feed.run(use_prefect=True)` is called inside a Prefect flow, the dataflows running on the Ray workers become its subflows.
