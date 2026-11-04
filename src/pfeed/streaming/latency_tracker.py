from __future__ import annotations

import time


class LatencyTracker:
    """Tracks a stream's latency in windows of log_interval seconds.

    A message's latency is from when the stream API received it to when it is tracked, e.g. after all its transformations,
    so it includes the queueing in the main process (and in zmq and the Ray worker); a rising p50 means the stream is falling behind.
    """

    def __init__(self, log_interval: float):
        self._log_interval = log_interval
        self._latencies_ms: list[float] = []
        self._window_start = 0.0

    def track(self, received_at: int) -> str | None:
        """Records one message's latency, returns the window's stats once it has lasted log_interval seconds.

        Args:
            received_at: when the message was received, in ns since the epoch
        """
        now = time.monotonic()
        if not self._window_start:
            self._window_start = now
        self._latencies_ms.append((time.time_ns() - received_at) / 1e6)
        if (elapsed := now - self._window_start) < self._log_interval:
            return None
        latencies = sorted(self._latencies_ms)
        n = len(latencies)
        self._latencies_ms.clear()
        self._window_start = now
        return (
            f"{n / elapsed:.1f} msg/s | latency "
            + f"p50={latencies[n // 2]:.1f}ms p99={latencies[min(n - 1, int(n * 0.99))]:.1f}ms max={latencies[-1]:.1f}ms"
        )
