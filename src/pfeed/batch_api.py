"""DRAFT, not implemented: the batch API's backfill interface, nothing uses it yet.

Backfilling completes a time range of stored data, whatever is missing in it and wherever it came from.
The caller doesn't need to know where the holes are; existing data is kept, only the missing data is added.
The main difference from download() is the time range: everywhere else in pfeed is per date (whole dates are re-downloaded),
backfill takes any time range, e.g. within a day, so the range doesn't have to be re-downloaded.
It is a data management verb of pfeed, not trading-specific, which is why it belongs to pfeed's batch side and not to pfund, e.g.:
- after a download: (d1, d5) was downloaded, but d3 10:00-10:30 is missing, e.g. from a provider that
  only returns 500 rows per call, so pfeed fetches per date internally but the actual calls are per minute and one failed;
  backfill (d3 10:00, d3 10:30) and (d1, d5) is complete, instead of re-downloading the whole d3 (hundreds of calls for 30 minutes).
  NOTE: a gap of whole dates, e.g. all of d3, is just a re-download of d3, not a backfill
- while streaming: the connection dropped or the process was down, so the stored streamed data has a gap,
  e.g. a strategy restarting during live trading would warm up from incomplete data

Either way, backfilling:
1. reads the stored data of the range and finds what's missing
2. fetches it from the venue's API (most likely REST) via the batch API's backfill()
3. merges it with the stored data (removing duplicates) and writes it back

Split of responsibilities:
- batch API's backfill(): a pure fetch for the given params, a faithful mirror of the venue's response, no cleaning,
  e.g. pfeed-bybit's batch API calls pfund-bybit's REST API (today it only downloads Bybit's daily dump files,
  which don't have today's data until the day ends)
- feed: a public backfill() verb, like download(), for the standalone usage
- data engine (BackfillCoordinator, see pfeed's CLAUDE.md): one caller of it while streaming, decides the params,
  i.e. what's missing and when to fetch, coordinates across feeds (e.g. per-source rate limits, all feeds silent = local network)
- handler/IO: the merge, as an idempotent write that removes duplicates

Open questions:
- finding what's missing: bars have a known cadence, so missing bars can be found; ticks don't,
  so a tick range can only be refetched as a whole and merged without duplicates
- naming: the feed's verb is backfill(), the batch API's method is then just a range fetch (e.g. get_data(start_ts, end_ts)),
  which download() could also use for providers with a row limit
- everything above the source is date-based (requests, split_by_date(), find_missing_dates_in_storage()),
  backfill needs ranges within a day, e.g. 10:00-10:30
- the REST response's schema differs from the daily dump's, so it needs its own cleaning transformations in the feed
- ticks can't always be recovered, e.g. a REST endpoint returning only the latest N trades
- backfill at startup (from the last stored timestamp to now), or only for gaps while streaming
- sync or async
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import polars as pl
    from pfund_blocks.entities.products.product_base import BaseProduct
    from pfund_blocks.resolution import Resolution


class SupportsBackfill(Protocol):
    def backfill(
        self,
        product: BaseProduct,
        resolution: Resolution,
        start_ts: int,
        end_ts: int,
    ) -> pl.DataFrame | None:
        """Fetches the venue's data of the product in [start_ts, end_ts), as int ns timestamps.

        Returns:
            The venue's data as is (not cleaned), None if the venue has no data in the range.
        """
        ...
