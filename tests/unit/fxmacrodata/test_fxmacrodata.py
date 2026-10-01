from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

import pfeed as pe


def _page(rows: list[dict[str, Any]], has_more: bool, **pagination: Any):
    return {
        "currency": "USD",
        "indicator": "inflation",
        "pagination": {"has_more": has_more, **pagination},
        "freemium_window": {"applied": True, "max_days": 90},
        "data": rows,
    }


def test_download_follows_pagination_and_keeps_announcement_columns():
    requested: list[tuple[str, Mapping[str, str]]] = []

    def request_json(url: str, headers: Mapping[str, str]) -> Mapping[str, Any]:
        requested.append((url, headers))
        if "offset=0" in url:
            return _page(
                [
                    {
                        "announcement_id": "usd_inflation_2026-07-31",
                        "date": "2026-07-31",
                        "val": 3.4,
                        "announcement_datetime": 1786537800,
                    }
                ],
                has_more=True,
                next_offset=1,
            )
        return _page(
            [
                {
                    "announcement_id": "usd_inflation_2026-08-31",
                    "date": "2026-08-31",
                    "val": 3.4,
                    "announcement_datetime": 1789216200,
                }
            ],
            has_more=False,
        )

    feed = pe.FXMacroData().announcement_feed
    feed._api_key = None
    feed._request_json = request_json

    frame = feed.download(
        currency="usd",
        indicator="inflation",
        start_date="2026-07-01",
        end_date="2026-09-30",
        limit=1,
    ).collect()

    assert frame.columns == ["announcement_id", "date", "val", "announcement_datetime"]
    assert frame.get_column("announcement_id").to_list() == [
        "usd_inflation_2026-07-31",
        "usd_inflation_2026-08-31",
    ]
    assert len(requested) == 2
    assert "/announcements/USD/inflation?" in requested[0][0]
    assert "start_date=2026-07-01" in requested[0][0]
    assert "limit=1" in requested[0][0]
    assert "offset=1" in requested[1][0]
    assert "X-API-Key" not in requested[0][1]
    assert feed.last_response_metadata["indicator"] == "inflation"
    assert feed.last_response_metadata["freemium_window"]["max_days"] == 90


def test_download_falls_back_to_row_count_when_next_offset_is_missing():
    offsets: list[str] = []

    def request_json(url: str, headers: Mapping[str, str]) -> Mapping[str, Any]:
        offsets.append(url.split("offset=")[1].split("&")[0])
        row = {"announcement_id": f"row-{len(offsets)}", "val": 1.0}
        return _page([row, row], has_more=len(offsets) < 2)

    feed = pe.FXMacroData().announcement_feed
    feed._request_json = request_json

    frame = feed.download("USD", "inflation", limit=2).collect()

    assert offsets == ["0", "2"]
    assert frame.height == 4


def test_download_sends_api_key_in_header_only():
    requested: list[tuple[str, Mapping[str, str]]] = []

    def request_json(url: str, headers: Mapping[str, str]) -> Mapping[str, Any]:
        requested.append((url, headers))
        return {"pagination": {"has_more": False}, "data": []}

    feed = pe.FXMacroData().announcement_feed
    feed._api_key = "test-key"
    feed._request_json = request_json

    frame = feed.download("AUD", "inflation").collect()

    assert frame.is_empty()
    assert requested[0][1]["X-API-Key"] == "test-key"
    assert "test-key" not in requested[0][0]
    assert "api_key" not in requested[0][0]


def test_api_key_is_read_from_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FXMACRODATA_API_KEY", "env-test-key")

    feed = pe.FXMacroData().announcement_feed

    assert feed._api_key == "env-test-key"


@pytest.mark.parametrize("limit", [0, 101])
def test_download_rejects_page_size_outside_api_bounds(limit: int):
    feed = pe.FXMacroData().announcement_feed
    feed._request_json = lambda url, headers: pytest.fail("no request expected")

    with pytest.raises(ValueError, match="between 1 and 100"):
        feed.download("USD", "inflation", limit=limit)


def test_client_exposes_the_announcement_feed():
    client = pe.FXMD()

    assert isinstance(client, pe.FXMacroData)
    assert client.feeds == [client.announcement_feed]
    assert client.name == "FXMACRODATA"
