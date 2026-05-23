# pfeed — Claude Context

## Backfill belongs in `pfeed/engine.py`

Live gap detection and backfill (fetching missed data from `source.batch_api` and writing through `handler.write_batch`) is **engine's job**, not feed / handler / sink / storage.

Why:
- Correlated outages need cross-feed coordination (all feeds silent ⇒ local network, not per-source).
- Rate limits are per-source-global; only engine sees all feeds at once.
- Engine already owns lifecycle infra (`_zmq_thread`, run loop).
- Backfilled messages can ride the same zmq channel as live ones — same standardization → write path.

Implement as a separate `BackfillCoordinator` class that engine instantiates and lifecycle-manages, so engine stays single-responsibility.

Scheduled / offline historical backfill is a separate CLI command, not part of the engine run loop.
