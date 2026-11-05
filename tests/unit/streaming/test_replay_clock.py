"""ReplayClock, merging several participants' messages by time."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

import asyncio

import pytest

from pfeed.streaming.replay_clock import ReplayClock

NS_PER_SECOND = 1_000_000_000


async def _replay(clock: ReplayClock, participant: str, timestamps: list[int], released: list[tuple[str, int]], *, delay: float = 0):
    try:
        for ts in timestamps:
            await clock.wait_until(participant, ts)
            released.append((participant, ts))
            # e.g. the participant processing the message or reading its next day
            await asyncio.sleep(delay)
    finally:
        clock.leave(participant)


@pytest.mark.asyncio
async def test_messages_of_all_participants_are_released_in_time_order():
    clock = ReplayClock()
    clock.join('a')
    clock.join('b')
    released: list[tuple[str, int]] = []

    # 'b' is slower between its messages, its earlier messages must still come first
    await asyncio.gather(
        _replay(clock, 'a', [1, 4, 5], released),
        _replay(clock, 'b', [2, 3, 6], released, delay=0.01),
    )

    assert released == [('a', 1), ('b', 2), ('b', 3), ('a', 4), ('a', 5), ('b', 6)]


@pytest.mark.asyncio
async def test_ties_go_to_the_participant_that_joined_first():
    clock = ReplayClock()
    clock.join('a')
    clock.join('b')
    released: list[tuple[str, int]] = []

    await asyncio.gather(_replay(clock, 'b', [1, 2], released), _replay(clock, 'a', [1, 2], released))

    assert released == [('a', 1), ('b', 1), ('a', 2), ('b', 2)]


@pytest.mark.asyncio
async def test_a_failed_participant_leaving_releases_the_others():
    clock = ReplayClock()
    clock.join('a')
    clock.join('b')
    released: list[tuple[str, int]] = []

    async def fail() -> None:
        try:
            await asyncio.sleep(0.01)
            raise RuntimeError('replay failed')
        finally:
            clock.leave('b')

    results = await asyncio.gather(_replay(clock, 'a', [1, 2], released), fail(), return_exceptions=True)

    assert released == [('a', 1), ('a', 2)]
    assert isinstance(results[1], RuntimeError)


@pytest.mark.asyncio
async def test_wait_until_without_joining_raises():
    with pytest.raises(ValueError, match='has not joined'):
        await ReplayClock().wait_until('a', 1)


def test_joining_twice_raises():
    clock = ReplayClock()
    clock.join('a')
    with pytest.raises(ValueError, match='has already joined'):
        clock.join('a')


@pytest.mark.parametrize(('replay_pace', 'max_delay', 'delays'), [
    # ASAP
    (0, None, [0, 0, 0]),
    # fixed cadence
    (0.5, None, [0, 0.5, 0.5]),
    # realistic: the time between the messages
    (None, None, [0, 1, 3]),
    # realistic, at most max_delay
    (None, 2, [0, 1, 2]),
])
@pytest.mark.asyncio
async def test_pace_is_applied_across_participants(
    mocker: MockerFixture, replay_pace: float | None, max_delay: float | None, delays: list[float]
):
    sleep = asyncio.sleep
    slept: list[float] = []

    async def record_sleep(delay: float) -> None:
        slept.append(delay)
        await sleep(0)

    _ = mocker.patch('asyncio.sleep', new=record_sleep)
    clock = ReplayClock(replay_pace)
    clock.join('a')
    clock.join('b')

    async def replay(participant: str, timestamps: list[int]) -> None:
        try:
            for ts in timestamps:
                await clock.wait_until(participant, ts * NS_PER_SECOND, max_delay=max_delay)
        finally:
            clock.leave(participant)

    # the pace is the time since the previous message of any participant, i.e. 0s -> 1s (b) -> 4s (a)
    await asyncio.gather(replay('a', [0, 4]), replay('b', [1]))

    assert slept == delays
