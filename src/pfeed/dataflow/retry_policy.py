from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Retries of a batch extraction that failed with a transient error.

    Waits `wait * backoff**attempt` seconds (with jitter) between attempts.
    The default never retries, since no error is listed in `retry_on`.
    """

    # transient errors, e.g. timeouts and dropped connections of the source's API
    retry_on: tuple[type[Exception], ...] = ()
    max_retries: int = 3
    wait: float = 1.0
    backoff: float = 2.0
