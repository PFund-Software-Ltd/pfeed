from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Hashable

import asyncio

from pfund_blocks.utils.temporal import NS_PER_SECOND


class ReplayClock:
    """Orders the messages of several replays by the time they would have been received live, and paces them.

    Each participant (e.g. a feed replaying its stored data) calls wait_until() with the time of its next message,
    the clock lets through the earliest one of all participants, once every participant has told the time of its next message
    or has left, ties go to the participant that joined first.
    A participant has to join before any participant starts, otherwise its messages can come after later ones,
    and must leave when it ends (e.g. in a finally), even when failed or cancelled, otherwise the others wait for it forever.
    """

    def __init__(self, replay_pace: float | None = 0):
        """
        Args:
            replay_pace: Pacing between messages, see MarketFeed.replay().
                0 = ASAP, >0 = fixed cadence in seconds, None = realistic (the time between consecutive messages).
        """
        self._replay_pace = replay_pace
        # NOTE: dicts keep the insertion order, which is the join order for ties
        self._participants: dict[Hashable, int] = {}
        # the time of each participant's next message, a participant without one is busy, e.g. reading its next day
        self._next_ts: dict[Hashable, int] = {}
        self._prev_ts: int | None = None
        self._waiters: list[asyncio.Future[None]] = []

    @property
    def replay_pace(self) -> float | None:
        return self._replay_pace

    def join(self, participant: Hashable) -> None:
        if participant in self._participants:
            raise ValueError(f"{participant} has already joined the replay clock")
        self._participants[participant] = len(self._participants)

    def leave(self, participant: Hashable) -> None:
        """The participant has no more messages, e.g. its replay has ended or failed."""
        _ = self._participants.pop(participant, None)
        _ = self._next_ts.pop(participant, None)
        self._notify()

    def _notify(self) -> None:
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _is_next(self, participant: Hashable) -> bool:
        # NOTE: a busy participant's next message can be earlier than any of the others'
        if len(self._next_ts) < len(self._participants):
            return False
        order = self._participants
        earliest = min(self._next_ts, key=lambda p: (self._next_ts[p], order[p]))
        return earliest == participant

    async def wait_until(
        self, participant: Hashable, ts: int, max_delay: float | None = None
    ) -> None:
        """Waits until the participant's message at ts (int ns) is the next one of the replay, then sleeps the pace.

        Args:
            max_delay: Seconds the realistic pace sleeps at most, e.g. one bar period, so data gaps (e.g. weekends) aren't slept through.
        """
        if participant not in self._participants:
            raise ValueError(f"{participant} has not joined the replay clock")
        self._next_ts[participant] = ts
        self._notify()
        while not self._is_next(participant):
            waiter = asyncio.get_running_loop().create_future()
            self._waiters.append(waiter)
            await waiter
        if (
            self._replay_pace == 0 or self._prev_ts is None
        ):  # ASAP, or the first message
            delay = 0.0
        elif self._replay_pace is not None:  # fixed cadence
            delay = self._replay_pace
        else:  # realistic: the time between consecutive messages
            delay = (ts - self._prev_ts) / NS_PER_SECOND
            if max_delay is not None:
                delay = min(delay, max_delay)
        self._prev_ts = ts
        # NOTE: also awaited when delay is 0, so the consumers run between messages; a sync callback never yields,
        # an ASAP replay would otherwise fill the iteration queue of __aiter__(), which drops the oldest message when full
        await asyncio.sleep(delay)
        # NOTE: the participant is busy until it waits for its next message, so the others keep waiting for it
        del self._next_ts[participant]
