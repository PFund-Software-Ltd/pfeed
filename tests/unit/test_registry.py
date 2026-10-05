"""Tests pfeed.registry against fake "pfeed.sources" entry points, so no real plugin has to be installed."""

from typing import ClassVar

import sys
import types
from importlib.metadata import EntryPoint

import pytest

import pfeed as pe
from pfeed import registry
from pfeed.enums import DataCategory

FAKE_MODULE = "fake_pfeed_plugin"


class FakeSource:
    name = "FAKE"


class FakeMarketFeed:
    pass


class FakeClient:
    DataSource = FakeSource
    Feeds: ClassVar[dict] = {DataCategory.MARKET_DATA: FakeMarketFeed}


class MismatchedClient:
    # entry point is named "fake" but the client's source says otherwise
    DataSource = type("OtherSource", (), {"name": "OTHER"})
    Feeds: ClassVar[dict] = {}


def _ep(name: str, attr: str = "FakeClient") -> EntryPoint:
    return EntryPoint(name=name, value=f"{FAKE_MODULE}:{attr}", group=registry.ENTRY_POINT_GROUP)


@pytest.fixture
def install_plugins(monkeypatch: pytest.MonkeyPatch):
    """Replaces the installed entry points with the given fake ones."""
    module = types.ModuleType(FAKE_MODULE)
    for cls in (FakeClient, MismatchedClient):
        setattr(module, cls.__name__, cls)
    monkeypatch.setitem(sys.modules, FAKE_MODULE, module)

    def _install(*eps: EntryPoint):
        monkeypatch.setattr(registry, "entry_points", lambda group: [ep for ep in eps if ep.group == group])
        registry.get_entry_points.cache_clear()

    yield _install
    registry.get_entry_points.cache_clear()


def test_entry_point_names_are_uppercased(install_plugins):
    install_plugins(_ep("fake"))
    assert list(registry.get_entry_points()) == ["FAKE"]


def test_duplicate_source_names_raise(install_plugins):
    install_plugins(_ep("fake"), _ep("FAKE"))
    with pytest.raises(ValueError, match="registered by multiple packages"):
        registry.get_entry_points()


def test_list_sources_is_sorted(install_plugins):
    install_plugins(_ep("zeta"), _ep("alpha"))
    assert registry.list_sources() == ["ALPHA", "ZETA"]


def test_no_plugins_installed(install_plugins):
    install_plugins()
    assert registry.list_sources() == []
    with pytest.raises(ValueError, match="unknown data source"):
        registry.get_client("fake")


def test_get_entry_point_is_case_insensitive(install_plugins):
    install_plugins(_ep("fake"))
    assert registry.get_entry_point("Fake").name == "fake"


def test_get_entry_point_unknown_source(install_plugins):
    install_plugins(_ep("fake"))
    with pytest.raises(ValueError, match=r"unknown data source 'nope'.*\['FAKE'\]"):
        registry.get_entry_point("nope")


def test_get_client_and_source(install_plugins):
    install_plugins(_ep("fake"))
    assert registry.get_client("fake") is FakeClient
    assert registry.get_source("fake") is FakeSource


def test_get_client_rejects_mismatched_source_name(install_plugins):
    install_plugins(_ep("fake", attr="MismatchedClient"))
    with pytest.raises(ValueError, match="source name is OTHER"):
        registry.get_client("fake")


@pytest.mark.parametrize("data_category", [DataCategory.MARKET_DATA, "market_data", "MARKET_DATA"])
def test_get_feed(install_plugins, data_category):
    install_plugins(_ep("fake"))
    assert registry.get_feed("fake", data_category) is FakeMarketFeed


def test_get_feed_unsupported_category(install_plugins):
    install_plugins(_ep("fake"))
    with pytest.raises(ValueError, match="FAKE has no feed for"):
        registry.get_feed("fake", DataCategory.ANNOUNCEMENT_DATA)


def test_pe_exposes_client_by_class_name(install_plugins):
    install_plugins(_ep("fake"))
    assert pe.FakeClient is FakeClient
    assert "FakeClient" in dir(pe)


def test_pe_unknown_attribute_raises(install_plugins):
    install_plugins(_ep("fake"))
    with pytest.raises(AttributeError):
        _ = pe.fake
