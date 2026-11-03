"""DataEngine's async iteration over its streaming feeds, through stand-in feeds."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

import asyncio
import contextlib

import pytest

from pfeed.engine import DataEngine


class _Feed:
    def __init__(self, messages: list[Any], *, error: Exception | None = None, endless: bool = False):
        self.messages = messages
        self.error = error
        self.endless = endless
        self.closed = False

    def is_streaming(self) -> bool:
        return True

    async def __aiter__(self) -> AsyncGenerator[Any]:
        try:
            for msg in self.messages:
                await asyncio.sleep(0)
                yield msg
            if self.error:
                raise self.error
            if self.endless:
                await asyncio.Event().wait()
        finally:
            self.closed = True


def _engine(*feeds: _Feed) -> DataEngine:
    engine = DataEngine()
    engine._feeds = cast("Any", list(feeds))
    return engine


@pytest.mark.asyncio
async def test_aiter_yields_every_feeds_messages_until_all_end():
    engine = _engine(_Feed([1, 2]), _Feed(['a', 'b', 'c']))
    received = [msg async for msg in engine]
    assert sorted(map(str, received)) == ['1', '2', 'a', 'b', 'c']


@pytest.mark.asyncio
async def test_aiter_reraises_a_feeds_error_and_ends_the_others():
    endless = _Feed([], endless=True)
    engine = _engine(_Feed([1], error=RuntimeError('stream failed')), endless)
    with pytest.raises(RuntimeError, match='stream failed'):
        _ = [msg async for msg in engine]
    assert endless.closed


@pytest.mark.asyncio
async def test_aiter_closing_after_break_ends_the_feeds():
    feeds = [_Feed([1], endless=True), _Feed([2], endless=True)]
    async with contextlib.aclosing(aiter(_engine(*feeds))) as messages:
        async for _ in messages:
            break
    assert all(feed.closed for feed in feeds)
