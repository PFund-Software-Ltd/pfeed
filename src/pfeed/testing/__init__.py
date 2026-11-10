"""Reusable test suites for pfeed data source plugins (pfeed-xxx).

pfeed owns the assertions, each plugin repo runs them against its real vendor
by subclassing a suite, e.g. in pfeed-xxx/tests/e2e/test_market_feed.py:

    import pytest
    from pfeed.testing import MarketFeedCase, MarketFeedE2ETests
    from pfeed_xxx import Xxx

    pytestmark = pytest.mark.network

    class TestXxxMarketFeed(MarketFeedE2ETests):
        source = Xxx
        start_date, end_date = "2026-09-01", "2026-09-02"
        cases = [MarketFeedCase("BTC_USDT_PERP", "1t"), MarketFeedCase("BTC_USDT_FUT", "1m", {"expiration": "2026-09-25"})]

A plugin with custom behavior overrides a test or a hook method (e.g. `expected_dates`),
or adds its own tests to the subclass. Vendor-specific tests (e.g. raw column drift) stay in the plugin.

Requires pytest, which is not a runtime dependency of pfeed.
"""

import pytest

# installed modules aren't assert-rewritten by default, register before importing them for readable failures
pytest.register_assert_rewrite("pfeed.testing.market_feed")

from pfeed.testing.market_feed import MarketFeedCase, MarketFeedE2ETests  # noqa: E402

__all__ = ["MarketFeedCase", "MarketFeedE2ETests"]
