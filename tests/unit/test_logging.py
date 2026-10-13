"""Tests that pfeed derives a logger (i.e. a log file) per installed data source."""

import copy
import logging
from importlib.metadata import EntryPoint

import pytest

from pfeed import config, registry


@pytest.fixture
def fake_source(monkeypatch: pytest.MonkeyPatch):
    ep = EntryPoint(name="fake", value="fake_pfeed_plugin:FakeClient", group=registry.ENTRY_POINT_GROUP)
    monkeypatch.setattr(registry, "entry_points", lambda group: [ep] if group == ep.group else [])
    registry.get_entry_points.cache_clear()
    original = copy.deepcopy(config.get_logging_config())
    yield
    # restore the logging config and the loggers, so the fake source doesn't leak into other tests
    config.configure_logging(original)
    registry.get_entry_points.cache_clear()
    config.setup_logging(reset=True)


def test_installed_source_gets_its_own_logger(fake_source):
    config.setup_logging()
    logger = logging.getLogger("pfeed.fake")
    assert not logger.propagate
    assert logger.handlers
    # the derived entry is applied, not written back into the cached logging config
    assert "pfeed.fake" not in config.get_logging_config()["loggers"]


def test_explicit_logger_entry_wins(fake_source):
    config.configure_logging({"loggers": {"pfeed.fake": {"level": "WARNING"}}})
    config.setup_logging()
    assert logging.getLogger("pfeed.fake").level == logging.WARNING
